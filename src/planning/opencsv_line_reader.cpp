#include "planning/opencsv_line_reader.hpp"

#include "core/readers/hive_csv_options.hpp"
#include "core/readers/opencsv_line.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

void ReadOpenCSVLines(BaseFileReader &reader) {
	for (idx_t i = 0; i < reader.column_ids.size(); i++) {
		if (reader.column_ids[MultiFileLocalIndex(i)].GetId() == 0) {
			return;
		}
	}
	reader.column_ids.push_back(MultiFileLocalColumnId(0));
	reader.column_indexes.push_back(ColumnIndex(0));
}

void SplitOpenCSVLines(BaseFileReader &reader, const MultiFileReaderData &reader_data, DataChunk &chunk,
                       const HiveCSVOptions &options) {
	vector<bool> used(reader.column_ids.size(), false);
	for (auto &expr : reader_data.expressions) {
		if (!expr) {
			continue;
		}
		ExpressionIterator::VisitExpression<BoundReferenceExpression>(*expr, [&](const BoundReferenceExpression &ref) {
			if (ref.Index() < used.size()) {
				used[ref.Index()] = true;
			}
		});
	}
	auto file_columns = reader.GetColumns().size();
	optional_idx line_index;
	vector<pair<idx_t, idx_t>> targets;
	idx_t field_count = 0;
	for (idx_t i = 0; i < reader.column_ids.size(); i++) {
		auto file_column = reader.column_ids[MultiFileLocalIndex(i)].GetId();
		if (file_column >= file_columns) {
			continue;
		}
		if (file_column == 0 && !line_index.IsValid()) {
			line_index = i;
		}
		if (used[i]) {
			targets.emplace_back(i, file_column);
			field_count = MaxValue<idx_t>(field_count, file_column + 1);
		}
	}
	if (!line_index.IsValid()) {
		throw InternalException("Hive text table: the line of a line_fields file was not read");
	}
	if (targets.empty()) {
		return;
	}
	auto count = chunk.size();
	OpenCSVDialect dialect {options.delimiter, options.quote[0], options.escape[0]};
	vector<Vector> fields_read;
	for (idx_t t = 0; t < targets.size(); t++) {
		fields_read.emplace_back(LogicalType::VARCHAR, count);
	}
	{
		vector<VectorWriter<string_t>> writers;
		for (auto &vector : fields_read) {
			writers.push_back(FlatVector::Writer<string_t>(vector, count));
		}
		vector<string> fields(field_count);
		vector<bool> wanted(field_count, false);
		for (auto &target : targets) {
			wanted[target.second] = true;
		}
		vector<bool> valid;
		string buffer;
		for (auto line : chunk.data[line_index.GetIndex()].Values<string_t>()) {
			if (line.IsValid()) {
				auto &text = line.GetValue();
				SplitOpenCSVLine(text.GetData(), text.GetSize(), dialect, wanted, fields, valid, buffer);
			} else {
				valid.assign(field_count, false);
			}
			for (idx_t t = 0; t < targets.size(); t++) {
				auto file_column = targets[t].second;
				if (valid[file_column]) {
					auto &field = fields[file_column];
					writers[t].WriteValue(string_t(field.data(), UnsafeNumericCast<uint32_t>(field.size())));
				} else {
					writers[t].WriteNull();
				}
			}
		}
	}
	for (idx_t t = 0; t < targets.size(); t++) {
		chunk.data[targets[t].first].Reference(fields_read[t]);
	}
}

} // namespace duckdb
