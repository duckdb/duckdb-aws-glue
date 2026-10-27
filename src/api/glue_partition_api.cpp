#include "api/glue_api_util.hpp"
#include "api/glue_http_client.hpp"
#include "catalog/glue_transaction.hpp"

#include "duckdb/common/map.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parallel/task_executor.hpp"

#include <chrono>
#include <functional>
#include <thread>

#include <aws/glue/model/BatchCreatePartitionRequest.h>
#include <aws/glue/model/CreatePartitionRequest.h>
#include <aws/glue/model/DeletePartitionRequest.h>
#include <aws/glue/model/GetPartitionRequest.h>
#include <aws/glue/model/GetPartitionsRequest.h>
#include <aws/glue/model/GetTableRequest.h>
#include <aws/glue/model/Segment.h>
#include <aws/glue/model/UpdatePartitionRequest.h>

namespace duckdb {

//! UpdatePartition with the partition's storage descriptor as 'change' leaves it, everything else as it is
static void UpdatePartitionStorage(ClientContext &context, GlueCatalog &catalog,
                                   const std::shared_ptr<Aws::Glue::GlueClient> &client, const string &database_name,
                                   const string &table_name, const vector<string> &values,
                                   const std::function<void(Aws::Glue::Model::StorageDescriptor &)> &change) {
	GlueTransactionCache::InvalidateOnExit invalidate(context, catalog);
	CheckWritable(catalog, "UpdatePartition");
	GlueHttpClientContextScope http_scope(context);
	Aws::Glue::Model::GetPartitionRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetDatabaseName(database_name);
	get_request.SetTableName(table_name);
	get_request.SetPartitionValues(ToAwsValues(values));
	auto get_outcome = client->GetPartition(get_request);
	if (!get_outcome.IsSuccess()) {
		if (IsEntityNotFound(get_outcome)) {
			throw CatalogException("Partition [%s] does not exist in Glue table '%s.%s'",
			                       PartitionValuesToString(values), database_name, table_name);
		}
		ThrowGlueError(get_outcome, StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                               PartitionValuesToString(values)));
	}
	auto &partition = get_outcome.GetResult().GetPartition();
	auto storage_descriptor = partition.GetStorageDescriptor();
	change(storage_descriptor);
	Aws::Glue::Model::PartitionInput input;
	input.SetValues(partition.GetValues());
	input.SetStorageDescriptor(storage_descriptor);
	input.SetParameters(partition.GetParameters());
	Aws::Glue::Model::UpdatePartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionValueList(ToAwsValues(values));
	request.SetPartitionInput(input);
	auto outcome = client->UpdatePartition(request);
	if (!outcome.IsSuccess()) {
		ThrowGlueError(outcome, StringUtil::Format("UpdatePartition '%s.%s' [%s]", database_name, table_name,
		                                           PartitionValuesToString(values)));
	}
}

void GlueAPI::SetPartitionLocation(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                   const string &table_name, const vector<string> &values, const string &location) {
	UpdatePartitionStorage(context, catalog, GetClient(context, catalog), database_name, table_name, values,
	                       [&](Aws::Glue::Model::StorageDescriptor &storage) { storage.SetLocation(location); });
}

void GlueAPI::SetPartitionColumns(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                  const string &table_name, const vector<string> &values,
                                  const vector<GlueColumn> &columns) {
	UpdatePartitionStorage(
	    context, catalog, GetClient(context, catalog), database_name, table_name, values,
	    [&](Aws::Glue::Model::StorageDescriptor &storage) { storage.SetColumns(ToAwsColumns(columns)); });
}

bool GlueAPI::GetPartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                           const string &table_name, const vector<string> &values, GluePartitionInfo &result) {
	GluePartitionDetails details;
	if (!GetPartition(context, catalog, database_name, table_name, values, details)) {
		return false;
	}
	result.values = std::move(details.values);
	result.location = std::move(details.location);
	return true;
}

bool GlueAPI::GetPartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                           const string &table_name, const vector<string> &values, GluePartitionDetails &result,
                           string *raw_json) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::GetPartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionValues(ToAwsValues(values));
	auto outcome = client->GetPartition(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			return false;
		}
		ThrowGlueError(outcome, StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                           PartitionValuesToString(values)));
	}
	auto &partition = outcome.GetResult().GetPartition();
	result.values.clear();
	for (auto &value : partition.GetValues()) {
		result.values.push_back(ToStdString(value));
	}
	auto &storage_descriptor = partition.GetStorageDescriptor();
	result.location = ToStdString(storage_descriptor.GetLocation());
	result.serde_library = ToStdString(storage_descriptor.GetSerdeInfo().GetSerializationLibrary());
	result.serde_parameters = ToStdMap(storage_descriptor.GetSerdeInfo().GetParameters());
	result.columns = ToColumns(storage_descriptor.GetColumns());
	result.parameters = ToStdMap(partition.GetParameters());
	if (raw_json) {
		*raw_json = ToStdString(partition.Jsonize().View().WriteReadable());
	}
	return true;
}

bool GlueAPI::CreatePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              const string &table_name, const GluePartitionInput &partition, bool if_not_exists) {
	GlueTransactionCache::InvalidateOnExit invalidate(context, catalog);
	CheckWritable(catalog, "CreatePartition");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);

	// A partition carries its own StorageDescriptor: the table's, with the partition's location
	Aws::Glue::Model::GetTableRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetDatabaseName(database_name);
	get_request.SetName(table_name);
	auto get_outcome = client->GetTable(get_request);
	if (!get_outcome.IsSuccess()) {
		ThrowGlueError(get_outcome, StringUtil::Format("GetTable '%s.%s'", database_name, table_name));
	}
	auto descriptor = get_outcome.GetResult().GetTable().GetStorageDescriptor();
	descriptor.SetLocation(partition.location);

	Aws::Glue::Model::PartitionInput input;
	input.SetValues(ToAwsValues(partition.values));
	input.SetStorageDescriptor(descriptor);
	Aws::Glue::Model::CreatePartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionInput(input);
	auto outcome = client->CreatePartition(request);
	if (!outcome.IsSuccess()) {
		if (IsAlreadyExists(outcome)) {
			if (if_not_exists) {
				return false;
			}
			throw CatalogException("Partition [%s] already exists in Glue table '%s.%s'",
			                       PartitionValuesToString(partition.values), database_name, table_name);
		}
		ThrowGlueError(outcome, StringUtil::Format("CreatePartition '%s.%s' [%s]", database_name, table_name,
		                                           PartitionValuesToString(partition.values)));
	}
	return true;
}

bool GlueAPI::DeletePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              const string &table_name, const vector<string> &values) {
	GlueTransactionCache::InvalidateOnExit invalidate(context, catalog);
	CheckWritable(catalog, "DeletePartition");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	Aws::Glue::Model::DeletePartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionValues(ToAwsValues(values));
	auto outcome = client->DeletePartition(request);
	if (!outcome.IsSuccess()) {
		if (IsEntityNotFound(outcome)) {
			return false;
		}
		ThrowGlueError(outcome, StringUtil::Format("DeletePartition '%s.%s' [%s]", database_name, table_name,
		                                           PartitionValuesToString(values)));
	}
	return true;
}

void GlueAPI::RenamePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              const string &table_name, const vector<string> &values,
                              const vector<string> &new_values) {
	GlueTransactionCache::InvalidateOnExit invalidate(context, catalog);
	CheckWritable(catalog, "UpdatePartition");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);

	// the partition to rename, with everything it carries (location, SerDe, parameters)
	Aws::Glue::Model::GetPartitionRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetDatabaseName(database_name);
	get_request.SetTableName(table_name);
	get_request.SetPartitionValues(ToAwsValues(values));
	auto get_outcome = client->GetPartition(get_request);
	if (!get_outcome.IsSuccess()) {
		if (IsEntityNotFound(get_outcome)) {
			throw CatalogException("Partition [%s] does not exist in Glue table '%s.%s'",
			                       PartitionValuesToString(values), database_name, table_name);
		}
		ThrowGlueError(get_outcome, StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                               PartitionValuesToString(values)));
	}
	auto &partition = get_outcome.GetResult().GetPartition();

	// the new values must be free
	Aws::Glue::Model::GetPartitionRequest check_request;
	SetCatalogId(check_request, catalog);
	check_request.SetDatabaseName(database_name);
	check_request.SetTableName(table_name);
	check_request.SetPartitionValues(ToAwsValues(new_values));
	auto check_outcome = client->GetPartition(check_request);
	if (check_outcome.IsSuccess()) {
		throw CatalogException("Partition [%s] already exists in Glue table '%s.%s'",
		                       PartitionValuesToString(new_values), database_name, table_name);
	}
	if (!IsEntityNotFound(check_outcome)) {
		ThrowGlueError(check_outcome, StringUtil::Format("GetPartition '%s.%s' [%s]", database_name, table_name,
		                                                 PartitionValuesToString(new_values)));
	}

	Aws::Glue::Model::PartitionInput input;
	input.SetValues(ToAwsValues(new_values));
	input.SetStorageDescriptor(partition.GetStorageDescriptor());
	input.SetParameters(partition.GetParameters());
	Aws::Glue::Model::UpdatePartitionRequest request;
	SetCatalogId(request, catalog);
	request.SetDatabaseName(database_name);
	request.SetTableName(table_name);
	request.SetPartitionValueList(ToAwsValues(values));
	request.SetPartitionInput(input);
	auto outcome = client->UpdatePartition(request);
	if (!outcome.IsSuccess()) {
		ThrowGlueError(outcome,
		               StringUtil::Format("UpdatePartition '%s.%s' [%s] -> [%s]", database_name, table_name,
		                                  PartitionValuesToString(values), PartitionValuesToString(new_values)));
	}
}

//! The partitions Glue returns per request. 1000 is the maximum the API allows.
static constexpr int GLUE_PARTITIONS_PAGE_SIZE = 1000;
//! The requests to run at the same time when the catalog is AWS and the setting leaves the choice open
static constexpr idx_t GLUE_DEFAULT_PARTITION_SEGMENTS = 8;
//! The maximum Glue accepts for Segment::TotalSegments
static constexpr idx_t GLUE_MAX_PARTITION_SEGMENTS = 10;

//! One chain of GetPartitions requests: pages through the partitions of segment 'segment_number' (the whole table
//! when 'total_segments' is 1) and appends them to 'result'
static void FetchPartitionSegment(Aws::Glue::GlueClient &client, const GlueCatalog &catalog,
                                  const string &database_name, const string &table_name, int segment_number,
                                  int total_segments, vector<GluePartitionInfo> &result) {
	Aws::String next_token;
	do {
		Aws::Glue::Model::GetPartitionsRequest request;
		SetCatalogId(request, catalog);
		request.SetDatabaseName(database_name);
		request.SetTableName(table_name);
		// Only the values and the location of a partition are used. The column schema every partition repeats is a
		// large part of the response, and fewer bytes per partition means more partitions per page.
		request.SetExcludeColumnSchema(true);
		request.SetMaxResults(GLUE_PARTITIONS_PAGE_SIZE);
		if (total_segments > 1) {
			Aws::Glue::Model::Segment segment;
			segment.SetSegmentNumber(segment_number);
			segment.SetTotalSegments(total_segments);
			request.SetSegment(segment);
		}
		if (!next_token.empty()) {
			request.SetNextToken(next_token);
		}
		auto outcome = client.GetPartitions(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, StringUtil::Format("GetPartitions '%s.%s'", database_name, table_name));
		}
		auto &partitions = outcome.GetResult();
		for (auto &partition : partitions.GetPartitions()) {
			GluePartitionInfo info;
			for (auto &value : partition.GetValues()) {
				info.values.push_back(ToStdString(value));
			}
			info.location = ToStdString(partition.GetStorageDescriptor().GetLocation());
			result.push_back(std::move(info));
		}
		next_token = partitions.GetNextToken();
	} while (!next_token.empty());
}

class FetchPartitionSegmentTask : public BaseExecutorTask {
public:
	FetchPartitionSegmentTask(TaskExecutor &executor, ClientContext &context, Aws::Glue::GlueClient &client,
	                          const GlueCatalog &catalog, const string &database_name, const string &table_name,
	                          int segment_number, int total_segments, vector<GluePartitionInfo> &result)
	    : BaseExecutorTask(executor), context(context), client(client), catalog(catalog), database_name(database_name),
	      table_name(table_name), segment_number(segment_number), total_segments(total_segments), result(result) {
	}

	void ExecuteTask() override {
		// thread local: without it a worker's requests use the database level HTTP settings and stay out of this
		// connection's HTTP log
		GlueHttpClientContextScope http_scope(context);
		FetchPartitionSegment(client, catalog, database_name, table_name, segment_number, total_segments, result);
	}
	string TaskType() const override {
		return "FetchPartitionSegmentTask";
	}

private:
	ClientContext &context;
	Aws::Glue::GlueClient &client;
	const GlueCatalog &catalog;
	const string &database_name;
	const string &table_name;
	int segment_number;
	int total_segments;
	vector<GluePartitionInfo> &result;
};

//! The number of GetPartitions requests to run at the same time: the setting when it is given, otherwise one request
//! against a server given with ENDPOINT (moto ignores Segment and answers every segment with the whole table) and
//! GLUE_DEFAULT_PARTITION_SEGMENTS against AWS.
static int GetPartitionSegmentCount(ClientContext &context, const GlueCatalog &catalog) {
	idx_t segments = 0;
	Value setting;
	if (context.TryGetCurrentSetting("glue_get_partitions_segments", setting) && !setting.IsNull()) {
		segments = setting.GetValue<idx_t>();
	}
	if (segments == 0) {
		segments = catalog.options.endpoint.empty() ? GLUE_DEFAULT_PARTITION_SEGMENTS : 1;
	}
	if (segments > GLUE_MAX_PARTITION_SEGMENTS) {
		segments = GLUE_MAX_PARTITION_SEGMENTS;
	}
	return static_cast<int>(segments);
}

vector<GluePartitionInfo> GlueAPI::GetPartitions(ClientContext &context, GlueCatalog &catalog,
                                                 const string &database_name, const string &table_name) {
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	auto total_segments = GetPartitionSegmentCount(context, catalog);
	vector<vector<GluePartitionInfo>> segment_results(NumericCast<idx_t>(total_segments));

	if (total_segments == 1) {
		FetchPartitionSegment(*client, catalog, database_name, table_name, 0, 1, segment_results[0]);
	} else {
		// the segments do not overlap, so their requests can run at the same time
		TaskExecutor executor(context, TaskSchedulerType::ASYNC);
		for (int segment = 0; segment < total_segments; segment++) {
			executor.ScheduleTask(make_uniq<FetchPartitionSegmentTask>(
			    executor, context, *client, catalog, database_name, table_name, segment, total_segments,
			    segment_results[NumericCast<idx_t>(segment)]));
		}
		executor.WorkOnTasks();
	}

	vector<GluePartitionInfo> result;
	unordered_set<string> seen;
	for (auto &segment_result : segment_results) {
		for (auto &partition : segment_result) {
			// A Glue compatible server that ignores Segment answers every segment with the whole table: the values
			// identify the partition, so what was seen already is dropped here
			if (total_segments > 1 && !seen.insert(StringUtil::Join(partition.values, "\x1f")).second) {
				continue;
			}
			result.push_back(std::move(partition));
		}
	}
	return result;
}

//! Errors of single partitions in a BatchCreatePartition response that may pass when the partition is sent again
static bool IsTransientPartitionError(const string &code) {
	return code == "ThrottlingException" || code == "InternalServiceException" || code == "OperationTimeoutException" ||
	       code == "ConcurrentModificationException";
}

//! glue_debug_fail_partition_registration, '<error code>:<index>': the partition at 'index' of a registration and
//! every one after it fail with the error code; a transient one fails the first attempt only
struct InjectedPartitionFailure {
	string code;
	idx_t from = 0;
	bool enabled = false;

	static InjectedPartitionFailure Parse(const string &text) {
		InjectedPartitionFailure result;
		if (text.empty()) {
			return result;
		}
		auto separator = text.find(':');
		if (separator == string::npos || separator == 0 ||
		    !Value(text.substr(separator + 1)).DefaultTryCastAs(LogicalType::UBIGINT)) {
			throw InvalidInputException("glue_debug_fail_partition_registration '%s' is not '<error code>:<index>'",
			                            text);
		}
		result.code = text.substr(0, separator);
		result.from = Value(text.substr(separator + 1)).DefaultCastAs(LogicalType::UBIGINT).GetValue<uint64_t>();
		result.enabled = true;
		return result;
	}
	static InjectedPartitionFailure FromSettings(ClientContext &context) {
		Value setting;
		if (!context.TryGetCurrentSetting("glue_debug_fail_partition_registration", setting) || setting.IsNull()) {
			return InjectedPartitionFailure();
		}
		return Parse(setting.ToString());
	}
	bool Fails(idx_t index, idx_t attempt) const {
		return enabled && index >= from && (attempt == 0 || !IsTransientPartitionError(code));
	}
};

void GlueAPI::CheckDebugFailPartitionRegistration(ClientContext &, SetScope, Value &parameter) {
	if (!parameter.IsNull()) {
		InjectedPartitionFailure::Parse(parameter.ToString());
	}
}

void GlueAPI::BatchCreatePartitions(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                    const string &table_name, const vector<GluePartitionInput> &partitions) {
	GlueTransactionCache::InvalidateOnExit invalidate(context, catalog);
	CheckWritable(catalog, "BatchCreatePartition");
	if (partitions.empty()) {
		return;
	}
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
	auto injected = InjectedPartitionFailure::FromSettings(context);

	// A partition carries its own StorageDescriptor: the table's, with the partition's location
	Aws::Glue::Model::GetTableRequest get_request;
	SetCatalogId(get_request, catalog);
	get_request.SetDatabaseName(database_name);
	get_request.SetName(table_name);
	auto get_outcome = client->GetTable(get_request);
	if (!get_outcome.IsSuccess()) {
		ThrowGlueError(get_outcome, StringUtil::Format("GetTable '%s.%s'", database_name, table_name));
	}
	auto table_descriptor = get_outcome.GetResult().GetTable().GetStorageDescriptor();

	// BatchCreatePartition accepts at most 100 partitions per call. The SDK retries a failed request, not the single
	// partitions a successful response refuses, so those are sent again here when the error may pass.
	constexpr idx_t BATCH_SIZE = 100;
	constexpr idx_t MAX_ATTEMPTS = 4;
	struct PartitionError {
		idx_t index;
		string code;
		string message;
	};
	for (idx_t offset = 0; offset < partitions.size(); offset += BATCH_SIZE) {
		vector<idx_t> pending;
		for (idx_t i = offset; i < MinValue<idx_t>(offset + BATCH_SIZE, partitions.size()); i++) {
			pending.push_back(i);
		}
		for (idx_t attempt = 0; !pending.empty(); attempt++) {
			if (attempt > 0) {
				std::this_thread::sleep_for(std::chrono::milliseconds(100 << (2 * (attempt - 1))));
			}
			// the partition values of each partition sent, to match the errors of the response to them
			map<vector<string>, idx_t> sent;
			vector<PartitionError> errors;
			Aws::Vector<Aws::Glue::Model::PartitionInput> inputs;
			for (auto index : pending) {
				auto &partition = partitions[index];
				if (injected.Fails(index, attempt)) {
					errors.push_back({index, injected.code, "injected by glue_debug_fail_partition_registration"});
					continue;
				}
				Aws::Glue::Model::PartitionInput input;
				input.SetValues(ToAwsValues(partition.values));
				auto descriptor = table_descriptor;
				descriptor.SetLocation(partition.location);
				input.SetStorageDescriptor(descriptor);
				inputs.push_back(std::move(input));
				sent.emplace(partition.values, index);
			}
			if (!inputs.empty()) {
				Aws::Glue::Model::BatchCreatePartitionRequest request;
				SetCatalogId(request, catalog);
				request.SetDatabaseName(database_name);
				request.SetTableName(table_name);
				request.SetPartitionInputList(inputs);
				auto outcome = client->BatchCreatePartition(request);
				if (!outcome.IsSuccess()) {
					ThrowGlueError(outcome,
					               StringUtil::Format("BatchCreatePartition '%s.%s'", database_name, table_name));
				}
				for (auto &error : outcome.GetResult().GetErrors()) {
					vector<string> values;
					for (auto &value : error.GetPartitionValues()) {
						values.push_back(ToStdString(value));
					}
					auto code = ToStdString(error.GetErrorDetail().GetErrorCode());
					auto message = ToStdString(error.GetErrorDetail().GetErrorMessage());
					auto entry = sent.find(values);
					if (entry == sent.end()) {
						throw IOException("Glue BatchCreatePartition '%s.%s' failed for partition [%s], which is not "
						                  "among the partitions sent: %s (%s)",
						                  database_name, table_name, PartitionValuesToString(values), message, code);
					}
					if (code == "AlreadyExistsException") {
						// appending to an existing partition
						continue;
					}
					errors.push_back({entry->second, code, message});
				}
			}
			pending.clear();
			for (auto &error : errors) {
				if (!IsTransientPartitionError(error.code) || attempt + 1 == MAX_ATTEMPTS) {
					throw IOException(
					    "Glue BatchCreatePartition '%s.%s' failed for partition [%s]: %s: %s", database_name,
					    table_name, PartitionValuesToString(partitions[error.index].values), error.code, error.message);
				}
				pending.push_back(error.index);
			}
		}
	}
}

} // namespace duckdb
