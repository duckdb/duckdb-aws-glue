#include "api/glue_api_util.hpp"
#include "api/glue_http_client.hpp"

#include "duckdb/common/error_data.hpp"
#include "duckdb/common/numeric_utils.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/unordered_set.hpp"
#include "duckdb/main/client_context.hpp"

#include <aws/glue/model/BatchCreatePartitionRequest.h>
#include <aws/glue/model/BatchGetPartitionRequest.h>
#include <aws/glue/model/BatchUpdatePartitionRequest.h>
#include <aws/glue/model/CreatePartitionRequest.h>
#include <aws/glue/model/DeletePartitionRequest.h>
#include <aws/glue/model/GetPartitionRequest.h>
#include <aws/glue/model/GetPartitionsRequest.h>
#include <aws/glue/model/GetTableRequest.h>
#include <aws/glue/model/Segment.h>
#include <aws/glue/model/UpdatePartitionRequest.h>

namespace duckdb {

void GlueAPI::SetPartitionLocation(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                   const string &table_name, const vector<string> &values, const string &location) {
	CheckWritable(catalog, "UpdatePartition");
	GlueHttpClientContextScope http_scope(context);
	auto client = GetClient(context, catalog);
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
	storage_descriptor.SetLocation(location);
	Aws::Glue::Model::PartitionInput input;
	input.SetValues(partition.GetValues());
	input.SetStorageDescriptor(storage_descriptor);
	// the statistics describe the files at the old location
	auto parameters = ToStdMap(partition.GetParameters());
	RemoveBasicStatistics(parameters);
	input.SetParameters(ToAwsMap(parameters));
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

bool GlueAPI::GetPartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                           const string &table_name, const vector<string> &values, GluePartitionInfo &result) {
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
	result.values = ToStdValues(partition.GetValues());
	result.location = ToStdString(partition.GetStorageDescriptor().GetLocation());
	result.parameters = ToStdMap(partition.GetParameters());
	return true;
}

bool GlueAPI::CreatePartition(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                              const string &table_name, const GluePartitionInput &partition, bool if_not_exists) {
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

static string PartitionKey(const vector<string> &values) {
	return StringUtil::Join(values, "\x1f");
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
			info.values = ToStdValues(partition.GetValues());
			info.location = ToStdString(partition.GetStorageDescriptor().GetLocation());
			info.parameters = ToStdMap(partition.GetParameters());
			result.push_back(std::move(info));
		}
		next_token = partitions.GetNextToken();
	} while (!next_token.empty());
}

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
		// The segments do not overlap, so their requests can run at the same time. Every worker sets the context
		// scope itself: it is thread local, and without it the requests would use the database level HTTP settings
		// and stay out of this connection's HTTP log.
		vector<ErrorData> errors(NumericCast<idx_t>(total_segments));
		vector<thread> workers;
		for (int segment = 1; segment < total_segments; segment++) {
			workers.emplace_back([&, segment]() {
				GlueHttpClientContextScope worker_scope(context);
				try {
					FetchPartitionSegment(*client, catalog, database_name, table_name, segment, total_segments,
					                      segment_results[NumericCast<idx_t>(segment)]);
				} catch (std::exception &ex) {
					errors[NumericCast<idx_t>(segment)] = ErrorData(ex);
				}
			});
		}
		try {
			FetchPartitionSegment(*client, catalog, database_name, table_name, 0, total_segments, segment_results[0]);
		} catch (std::exception &ex) {
			errors[0] = ErrorData(ex);
		}
		for (auto &worker : workers) {
			worker.join();
		}
		for (auto &error : errors) {
			if (error.HasError()) {
				error.Throw();
			}
		}
	}

	vector<GluePartitionInfo> result;
	unordered_set<string> seen;
	for (auto &segment_result : segment_results) {
		for (auto &partition : segment_result) {
			// A Glue compatible server that ignores Segment answers every segment with the whole table: the values
			// identify the partition, so what was seen already is dropped here
			if (total_segments > 1 && !seen.insert(PartitionKey(partition.values)).second) {
				continue;
			}
			result.push_back(std::move(partition));
		}
	}
	return result;
}

static string TrimLocation(string location) {
	StringUtil::RTrim(location, "/");
	return location;
}

//! Add the statistics of the given (existing) partitions to the statistics Glue has for them
static void AddPartitionStatistics(Aws::Glue::GlueClient &client, const GlueCatalog &catalog,
                                   const string &database_name, const string &table_name,
                                   const vector<reference<const GluePartitionInput>> &partitions) {
	// BatchGetPartition accepts at most 1000 partitions per call, BatchUpdatePartition at most 100
	constexpr idx_t GET_BATCH_SIZE = 1000;
	constexpr idx_t UPDATE_BATCH_SIZE = 100;
	constexpr idx_t MAX_GET_ATTEMPTS = 5;
	unordered_map<string, reference<const GluePartitionInput>> inputs;
	for (auto &partition : partitions) {
		inputs.emplace(PartitionKey(partition.get().values), partition);
	}

	Aws::Vector<Aws::Glue::Model::BatchUpdatePartitionRequestEntry> updates;
	for (idx_t offset = 0; offset < partitions.size(); offset += GET_BATCH_SIZE) {
		Aws::Vector<Aws::Glue::Model::PartitionValueList> to_get;
		for (idx_t i = offset; i < MinValue<idx_t>(offset + GET_BATCH_SIZE, partitions.size()); i++) {
			Aws::Glue::Model::PartitionValueList values;
			values.SetValues(ToAwsValues(partitions[i].get().values));
			to_get.push_back(std::move(values));
		}
		// Glue returns the keys it did not get to (throttling) as unprocessed: ask again for those
		for (idx_t attempt = 0; attempt < MAX_GET_ATTEMPTS && !to_get.empty(); attempt++) {
			Aws::Glue::Model::BatchGetPartitionRequest request;
			SetCatalogId(request, catalog);
			request.SetDatabaseName(database_name);
			request.SetTableName(table_name);
			request.SetPartitionsToGet(to_get);
			auto outcome = client.BatchGetPartition(request);
			if (!outcome.IsSuccess()) {
				ThrowGlueError(outcome, StringUtil::Format("BatchGetPartition '%s.%s'", database_name, table_name));
			}
			for (auto &partition : outcome.GetResult().GetPartitions()) {
				auto input = inputs.find(PartitionKey(ToStdValues(partition.GetValues())));
				if (input == inputs.end()) {
					continue;
				}
				auto &partition_input = input->second.get();
				// files written to a directory the partition does not point at do not count for it
				auto &descriptor = partition.GetStorageDescriptor();
				if (TrimLocation(ToStdString(descriptor.GetLocation())) != TrimLocation(partition_input.location)) {
					continue;
				}
				auto parameters = ToStdMap(partition.GetParameters());
				GlueBasicStatistics statistics;
				if (!TryGetBasicStatistics(parameters, statistics)) {
					continue;
				}
				statistics.num_rows += partition_input.statistics->num_rows;
				statistics.num_files += partition_input.statistics->num_files;
				statistics.total_size += partition_input.statistics->total_size;
				SetBasicStatistics(parameters, statistics);

				Aws::Glue::Model::PartitionInput update_input;
				update_input.SetValues(partition.GetValues());
				update_input.SetStorageDescriptor(descriptor);
				update_input.SetParameters(ToAwsMap(parameters));
				if (partition.LastAccessTimeHasBeenSet()) {
					update_input.SetLastAccessTime(partition.GetLastAccessTime());
				}
				if (partition.LastAnalyzedTimeHasBeenSet()) {
					update_input.SetLastAnalyzedTime(partition.GetLastAnalyzedTime());
				}
				Aws::Glue::Model::BatchUpdatePartitionRequestEntry entry;
				entry.SetPartitionValueList(partition.GetValues());
				entry.SetPartitionInput(std::move(update_input));
				updates.push_back(std::move(entry));
			}
			to_get = outcome.GetResult().GetUnprocessedKeys();
		}
	}

	for (idx_t offset = 0; offset < updates.size(); offset += UPDATE_BATCH_SIZE) {
		auto batch_end = MinValue<idx_t>(offset + UPDATE_BATCH_SIZE, updates.size());
		Aws::Glue::Model::BatchUpdatePartitionRequest request;
		SetCatalogId(request, catalog);
		request.SetDatabaseName(database_name);
		request.SetTableName(table_name);
		request.SetEntries(Aws::Vector<Aws::Glue::Model::BatchUpdatePartitionRequestEntry>(
		    updates.begin() + NumericCast<int64_t>(offset), updates.begin() + NumericCast<int64_t>(batch_end)));
		auto outcome = client.BatchUpdatePartition(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, StringUtil::Format("BatchUpdatePartition '%s.%s'", database_name, table_name));
		}
		for (auto &error : outcome.GetResult().GetErrors()) {
			throw IOException("Glue BatchUpdatePartition '%s.%s' failed for partition [%s]: %s (%s)", database_name,
			                  table_name, PartitionValuesToString(ToStdValues(error.GetPartitionValueList())),
			                  ToStdString(error.GetErrorDetail().GetErrorMessage()),
			                  ToStdString(error.GetErrorDetail().GetErrorCode()));
		}
	}
}

void GlueAPI::BatchCreatePartitions(ClientContext &context, GlueCatalog &catalog, const string &database_name,
                                    const string &table_name, const vector<GluePartitionInput> &partitions) {
	CheckWritable(catalog, "BatchCreatePartition");
	if (partitions.empty()) {
		return;
	}
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
	auto table_descriptor = get_outcome.GetResult().GetTable().GetStorageDescriptor();

	// BatchCreatePartition accepts at most 100 partitions per call
	constexpr idx_t BATCH_SIZE = 100;
	vector<reference<const GluePartitionInput>> existing_with_statistics;
	for (idx_t offset = 0; offset < partitions.size(); offset += BATCH_SIZE) {
		auto batch_end = MinValue<idx_t>(offset + BATCH_SIZE, partitions.size());
		Aws::Vector<Aws::Glue::Model::PartitionInput> inputs;
		unordered_map<string, reference<const GluePartitionInput>> batch_partitions;
		for (idx_t i = offset; i < batch_end; i++) {
			auto &partition = partitions[i];
			Aws::Glue::Model::PartitionInput input;
			input.SetValues(ToAwsValues(partition.values));
			auto descriptor = table_descriptor;
			descriptor.SetLocation(partition.location);
			input.SetStorageDescriptor(descriptor);
			if (partition.statistics) {
				GlueParameters parameters;
				SetBasicStatistics(parameters, *partition.statistics);
				input.SetParameters(ToAwsMap(parameters));
				batch_partitions.emplace(PartitionKey(partition.values), partition);
			}
			inputs.push_back(std::move(input));
		}
		Aws::Glue::Model::BatchCreatePartitionRequest request;
		SetCatalogId(request, catalog);
		request.SetDatabaseName(database_name);
		request.SetTableName(table_name);
		request.SetPartitionInputList(inputs);
		auto outcome = client->BatchCreatePartition(request);
		if (!outcome.IsSuccess()) {
			ThrowGlueError(outcome, StringUtil::Format("BatchCreatePartition '%s.%s'", database_name, table_name));
		}
		for (auto &error : outcome.GetResult().GetErrors()) {
			auto code = ToStdString(error.GetErrorDetail().GetErrorCode());
			if (code == "AlreadyExistsException") {
				// appending to an existing partition
				auto partition = batch_partitions.find(PartitionKey(ToStdValues(error.GetPartitionValues())));
				if (partition != batch_partitions.end()) {
					existing_with_statistics.push_back(partition->second);
				}
				continue;
			}
			throw IOException("Glue BatchCreatePartition '%s.%s' failed for partition [%s]: %s (%s)", database_name,
			                  table_name, PartitionValuesToString(ToStdValues(error.GetPartitionValues())),
			                  ToStdString(error.GetErrorDetail().GetErrorMessage()), code);
		}
	}
	if (!existing_with_statistics.empty()) {
		AddPartitionStatistics(*client, catalog, database_name, table_name, existing_with_statistics);
	}
}

} // namespace duckdb
