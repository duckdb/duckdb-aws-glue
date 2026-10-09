#include "planning/stats/footer_stats.hpp"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/schema_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/table_function_catalog_entry.hpp"
#include "duckdb/common/error_data.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/vector/vector_iterator.hpp"
#include "duckdb/execution/execution_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/parallel/thread_context.hpp"
#include "duckdb/parser/tableref/table_function_ref.hpp"
#include "planning/hive_multi_file_reader.hpp"

namespace duckdb {

//! How many footers are read at a time
static constexpr idx_t FOOTER_STATISTICS_THREADS = 8;

//! What the row groups of the files say about one column
struct FooterColumnStatistics {
	idx_t row_groups = 0;
	//! The writer records a row group's dictionary size as its distinct count, when it dictionary-encoded the chunk
	idx_t row_groups_with_distinct_count = 0;
	idx_t max_distinct_count = 0;
	//! The bounds of an integer column over every row group, while each has them
	bool has_bounds = true;
	hugeint_t min = 0;
	hugeint_t max = 0;
};

struct FooterRowGroups {
	idx_t rows = 0;
	case_insensitive_map_t<FooterColumnStatistics> columns;

	void Merge(const FooterRowGroups &other) {
		rows += other.rows;
		for (auto &entry : other.columns) {
			auto &column = columns[entry.first];
			auto &source = entry.second;
			if (source.row_groups == 0) {
				continue;
			}
			column.has_bounds = column.has_bounds && source.has_bounds;
			if (column.row_groups == 0 || source.min < column.min) {
				column.min = source.min;
			}
			if (column.row_groups == 0 || source.max > column.max) {
				column.max = source.max;
			}
			column.row_groups += source.row_groups;
			column.row_groups_with_distinct_count += source.row_groups_with_distinct_count;
			column.max_distinct_count = MaxValue(column.max_distinct_count, source.max_distinct_count);
		}
	}
};

static optional_ptr<const TableFunction> GetSystemTableFunction(ClientContext &context, const string &name,
                                                                const LogicalType &argument) {
	auto &db = DatabaseInstance::GetDatabase(context);
	auto &system_catalog = Catalog::GetSystemCatalog(db);
	auto data = CatalogTransaction::GetSystemTransaction(db);
	auto &schema = system_catalog.GetSchema(data, Identifier::DefaultSchema());
	auto entry = schema.GetEntry(data, CatalogType::TABLE_FUNCTION_ENTRY, Identifier(name));
	if (!entry) {
		return nullptr;
	}
	return entry->Cast<TableFunctionCatalogEntry>().functions.GetFunctionByArguments(context, {argument});
}

//! Fold one chunk of parquet_metadata (a row per column chunk) into 'result'
static void AddFooterRows(DataChunk &chunk, const vector<Identifier> &names,
                          const case_insensitive_map_t<LogicalType> &data_columns, FooterRowGroups &result) {
	auto column = [&](const char *name) -> Vector & {
		for (idx_t i = 0; i < names.size(); i++) {
			if (names[i].GetIdentifierName() == name) {
				return chunk.data[i];
			}
		}
		throw InternalException("parquet_metadata has no column '%s'", name);
	};
	auto column_ids = column("column_id").Values<int64_t>();
	auto row_counts = column("row_group_num_rows").Values<int64_t>();
	auto paths = column("path_in_schema").Values<string_t>();
	auto distinct_counts = column("stats_distinct_count").Values<int64_t>();
	auto min_values = column("stats_min_value").Values<string_t>();
	auto max_values = column("stats_max_value").Values<string_t>();
	for (idx_t row = 0; row < chunk.size(); row++) {
		auto column_id = column_ids[row];
		if (column_id.IsValid() && column_id.GetValue() == 0 && row_counts[row].IsValid()) {
			result.rows += NumericCast<idx_t>(row_counts[row].GetValue());
		}
		if (!paths[row].IsValid()) {
			continue;
		}
		// a top-level column of the table; struct fields and list elements have dotted paths that match none
		auto path = paths[row].GetValue().GetString();
		auto type = data_columns.find(path);
		if (type == data_columns.end()) {
			continue;
		}
		auto &stats = result.columns[path];
		stats.row_groups++;
		auto distinct_count = distinct_counts[row];
		if (distinct_count.IsValid() && distinct_count.GetValue() > 0) {
			stats.row_groups_with_distinct_count++;
			stats.max_distinct_count =
			    MaxValue(stats.max_distinct_count, NumericCast<idx_t>(distinct_count.GetValue()));
		}
		if (!stats.has_bounds) {
			continue;
		}
		hugeint_t min;
		hugeint_t max;
		if (!type->second.IsIntegral() || !min_values[row].IsValid() || !max_values[row].IsValid() ||
		    !TryCast::Operation(min_values[row].GetValue(), min) ||
		    !TryCast::Operation(max_values[row].GetValue(), max)) {
			stats.has_bounds = false;
			continue;
		}
		if (stats.row_groups == 1 || min < stats.min) {
			stats.min = min;
		}
		if (stats.row_groups == 1 || max > stats.max) {
			stats.max = max;
		}
	}
}

//! Read the footers of 'files' with parquet_metadata, a few at a time. False when parquet_metadata is not available.
static bool ReadFooterRowGroups(ClientContext &context, const HiveScanInfo &info, const vector<OpenFileInfo> &files,
                                FooterRowGroups &result) {
	auto function = GetSystemTableFunction(context, "parquet_metadata", LogicalType::VARCHAR);
	if (!function || !function->init_local) {
		return false;
	}
	case_insensitive_map_t<LogicalType> data_columns;
	for (idx_t i = 0; i < info.names.size(); i++) {
		if (info.GetPartitionKeyIndex(info.names[i].GetIdentifierName()) == DConstants::INVALID_INDEX) {
			data_columns.emplace(info.names[i].GetIdentifierName(), info.types[i]);
		}
	}
	vector<Value> paths;
	for (auto &file : files) {
		paths.emplace_back(file.path);
	}
	vector<Value> inputs {Value::LIST(LogicalType::VARCHAR, std::move(paths))};
	named_argument_map_t named_parameters;
	vector<LogicalType> input_table_types;
	vector<Identifier> input_table_names;
	BoundTableFunction bound_function(*function);
	TableFunctionRef empty_ref;
	TableFunctionBindInput bind_input(inputs, named_parameters, input_table_types, input_table_names,
	                                  function->function_info.get(), nullptr, bound_function, empty_ref);
	vector<LogicalType> types;
	vector<Identifier> names;
	auto bind_data = function->bind(context, bind_input, types, names);
	vector<column_t> column_ids;
	for (idx_t i = 0; i < types.size(); i++) {
		column_ids.push_back(i);
	}
	TableFunctionInitInput init_input(bind_data.get(), column_ids, vector<idx_t>(), nullptr);
	auto global_state = function->init_global(context, init_input);

	auto thread_count = MinValue<idx_t>(files.size(), FOOTER_STATISTICS_THREADS);
	vector<FooterRowGroups> thread_results(thread_count);
	vector<ErrorData> errors(thread_count);
	auto read_footers = [&](idx_t thread_index) {
		try {
			ThreadContext thread_context(context);
			ExecutionContext execution_context(context, thread_context, nullptr);
			auto local_state = function->init_local(execution_context, init_input, global_state.get());
			DataChunk chunk;
			chunk.Initialize(Allocator::Get(context), types);
			while (true) {
				chunk.Reset();
				TableFunctionInput input(bind_data.get(), local_state.get(), global_state.get());
				function->function(context, input, chunk);
				if (chunk.size() == 0) {
					break;
				}
				AddFooterRows(chunk, names, data_columns, thread_results[thread_index]);
			}
		} catch (std::exception &ex) {
			errors[thread_index] = ErrorData(ex);
		}
	};
	vector<thread> workers;
	for (idx_t i = 1; i < thread_count; i++) {
		workers.emplace_back(read_footers, i);
	}
	read_footers(0);
	for (auto &worker : workers) {
		worker.join();
	}
	for (auto &error : errors) {
		if (error.HasError()) {
			error.Throw();
		}
	}
	for (auto &thread_result : thread_results) {
		result.Merge(thread_result);
	}
	return true;
}

//! The table's rows and the distinct counts its footers give: the dictionary size where every row group has one (a
//! lower bound, and exact for the low-cardinality columns that equality filters and composite joins use), else the
//! width of an integer column's bounds
static void SetDistinctCounts(const FooterRowGroups &footers, HiveFooterStatistics &result) {
	result.rows = footers.rows;
	if (footers.rows == 0) {
		return;
	}
	for (auto &entry : footers.columns) {
		auto &column = entry.second;
		if (column.row_groups == 0) {
			continue;
		}
		idx_t distinct_count = 0;
		if (column.row_groups_with_distinct_count == column.row_groups) {
			distinct_count = column.max_distinct_count;
		}
		// over several row groups the largest dictionary only bounds the count from below
		if (column.has_bounds && (distinct_count == 0 || column.row_groups > 1)) {
			auto width = column.max - column.min + 1;
			distinct_count =
			    MaxValue(distinct_count, width >= hugeint_t(footers.rows) ? footers.rows : Hugeint::Cast<idx_t>(width));
		}
		if (distinct_count > 0) {
			result.distinct_counts[entry.first] = MinValue(distinct_count, footers.rows);
		}
	}
}

bool ReadFooterStatistics(ClientContext &context, const HiveScanInfo &info, const vector<OpenFileInfo> &files,
                          HiveFooterStatistics &result) {
	FooterRowGroups row_groups;
	if (!ReadFooterRowGroups(context, info, files, row_groups)) {
		return false;
	}
	SetDistinctCounts(row_groups, result);
	return true;
}

} // namespace duckdb
