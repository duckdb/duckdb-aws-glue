#include "planning/stats/hive_footer_statistics.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/logging/logger.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"
#include "planning/hive_multi_file_reader.hpp"
#include "planning/hive_query_cache.hpp"

namespace duckdb {

//! As UnionByReaderTask in core, which is not exported and also merges the files' schemas
class HiveFooterReadTask : public BaseExecutorTask {
public:
	HiveFooterReadTask(TaskExecutor &executor, HiveParquetFooters &footers, const OpenFileInfo &file)
	    : BaseExecutorTask(executor), footers(footers), file(file) {
	}

	void ExecuteTask() override {
		footers.Get(file);
	}
	string TaskType() const override {
		return "HiveFooterReadTask";
	}

private:
	HiveParquetFooters &footers;
	const OpenFileInfo &file;
};

static idx_t FooterStatisticsMaxFiles(ClientContext &context) {
	Value setting;
	if (context.TryGetCurrentSetting("hive_footer_statistics_max_files", setting) && !setting.IsNull()) {
		// SET checks the bound; DBConfig::SetOptionByName does not
		return MinValue<idx_t>(setting.GetValue<idx_t>(), HIVE_FOOTER_STATISTICS_MAX_FILES_LIMIT);
	}
	return HIVE_FOOTER_STATISTICS_DEFAULT_MAX_FILES;
}

HiveParquetFooters::HiveParquetFooters(ClientContext &context, const MultiFileBindData &bind_data,
                                       const HiveMultiFileList &list)
    : context(context), bind_data(bind_data), list(list), file_options(bind_data.file_options) {
	// the options the scan was bound with: can_have_nan and binary_as_string change what the statistics say
	options = bind_data.interface->InitializeOptions(context, nullptr);
	for (auto &parameter : list.ScanInfo().reader_parameters) {
		bind_data.interface->ParseOption(context, parameter.first, parameter.second, file_options, *options);
	}
}

shared_ptr<HiveFileFooterEntry> HiveParquetFooters::Entry(const OpenFileInfo &file) const {
	auto key = file.path;
	for (auto &parameter : list.ScanInfo().reader_parameters) {
		key += "\x1f" + parameter.first.GetIdentifierName() + "=" + parameter.second.ToString();
	}
	return HiveQueryCache::Get(context)->GetFileFooter(key);
}

shared_ptr<BaseUnionData> HiveParquetFooters::Get(const OpenFileInfo &file) {
	auto entry = Entry(file);
	annotated_lock_guard<annotated_mutex> guard(entry->lock);
	if (entry->read) {
		return entry->footer;
	}
	try {
		auto reader =
		    bind_data.multi_file_reader->CreateReader(context, file, *options, file_options, *bind_data.interface);
		if (reader) {
			// an index other than 0 keeps the footer only, not the reader
			entry->footer = reader->GetUnionData(1);
		}
	} catch (std::exception &ex) {
		ErrorData error(ex);
		if (error.Type() == ExceptionType::INTERRUPT) {
			throw;
		}
		// costing must not fail a query: the scan reports a file it cannot read
		DUCKDB_LOG_WARNING(context, "Could not read the footer of '%s' of Hive table '%s': %s", file.path,
		                   list.ScanInfo().Describe(), error.RawMessage());
	}
	entry->read = true;
	return entry->footer;
}

bool HiveParquetFooters::ReadAll() {
	auto max_files = FooterStatisticsMaxFiles(context);
	// a partitioned scan's listing would read its partitions before a join's filter prunes them
	if (max_files == 0 || !list.ScanInfo().partition_keys.empty()) {
		return false;
	}
	auto count = list.GetFileCount(max_files + 1);
	if (count.type != FileExpansionType::ALL_FILES_EXPANDED || count.count > max_files) {
		return false;
	}
	files = list.GetAllFiles();
	vector<reference<const OpenFileInfo>> unread;
	for (auto &file : files) {
		auto entry = Entry(file);
		annotated_lock_guard<annotated_mutex> guard(entry->lock);
		if (!entry->read) {
			unread.push_back(file);
		}
	}
	if (unread.size() > 1) {
		TaskExecutor executor(context, TaskSchedulerType::ASYNC);
		for (auto &file : unread) {
			executor.ScheduleTask(make_uniq<HiveFooterReadTask>(executor, *this, file.get()));
		}
		executor.WorkOnTasks();
	}
	return true;
}

optional_idx HiveParquetFooters::Rows() {
	if (!ReadAll()) {
		return optional_idx();
	}
	idx_t rows = 0;
	for (auto &file : files) {
		auto footer = Get(file);
		auto file_rows = footer ? footer->TryGetCardinalityEstimate() : optional_idx();
		if (!file_rows.IsValid()) {
			return optional_idx();
		}
		rows += file_rows.GetIndex();
	}
	return rows;
}

unique_ptr<BaseStatistics> HiveParquetFooters::Statistics(const MultiFileColumnDefinition &column) {
	if (!ReadAll() || files.empty()) {
		return nullptr;
	}
	unique_ptr<BaseStatistics> result;
	for (auto &file : files) {
		auto footer = Get(file);
		if (!footer) {
			return nullptr;
		}
		auto rows = footer->TryGetCardinalityEstimate();
		if (rows.IsValid() && rows.GetIndex() == 0) {
			// an empty file has no statistics, and no values the merged bounds must cover
			continue;
		}
		unique_ptr<BaseStatistics> statistics;
		try {
			statistics = footer->GetStatistics(context, column.name);
		} catch (std::exception &ex) {
			ErrorData error(ex);
			if (error.Type() == ExceptionType::INTERRUPT) {
				throw;
			}
			// costing must not fail a query: the scan reports a file it cannot read
			DUCKDB_LOG_WARNING(context, "Could not read the statistics of column '%s' of '%s' of Hive table '%s': %s",
			                   column.name.GetIdentifierName(), file.path, list.ScanInfo().Describe(),
			                   error.RawMessage());
			return nullptr;
		}
		if (!statistics || statistics->GetType() != column.type) {
			return nullptr;
		}
		if (!result) {
			result = std::move(statistics);
		} else {
			result->Merge(*statistics);
		}
	}
	return result;
}

} // namespace duckdb
