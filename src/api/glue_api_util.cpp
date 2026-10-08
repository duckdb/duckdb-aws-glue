#include "api/glue_api_util.hpp"

#include "duckdb/common/string_util.hpp"

namespace duckdb {

string ToStdString(const Aws::String &input) {
	return string(input.c_str(), input.size());
}

unordered_map<string, string> ToStdMap(const Aws::Map<Aws::String, Aws::String> &input) {
	unordered_map<string, string> result;
	for (auto &entry : input) {
		result.emplace(ToStdString(entry.first), ToStdString(entry.second));
	}
	return result;
}

vector<GlueColumn> ToColumns(const Aws::Vector<Aws::Glue::Model::Column> &input) {
	vector<GlueColumn> result;
	for (auto &column : input) {
		// Glue keeps the columns of earlier schemas of an Iceberg table, marked as no longer current
		auto &parameters = column.GetParameters();
		auto current = parameters.find("iceberg.field.current");
		if (current != parameters.end() && current->second == "false") {
			continue;
		}
		GlueColumn glue_column;
		glue_column.name = ToStdString(column.GetName());
		glue_column.type = ToStdString(column.GetType());
		glue_column.comment = ToStdString(column.GetComment());
		result.push_back(std::move(glue_column));
	}
	return result;
}

GlueDatabaseInfo ToDatabaseInfo(const Aws::Glue::Model::Database &database) {
	GlueDatabaseInfo result;
	result.name = ToStdString(database.GetName());
	result.description = ToStdString(database.GetDescription());
	result.location_uri = ToStdString(database.GetLocationUri());
	result.parameters = ToStdMap(database.GetParameters());
	return result;
}

GlueTableInfo ToTableInfo(const Aws::Glue::Model::Table &table) {
	GlueTableInfo result;
	result.name = ToStdString(table.GetName());
	result.database_name = ToStdString(table.GetDatabaseName());
	result.glue_table_type = ToStdString(table.GetTableType());
	result.table_type = GlueTableTypeFromString(result.glue_table_type);
	if (table.ViewOriginalTextHasBeenSet()) {
		result.view_original_text = ToStdString(table.GetViewOriginalText());
	}
	if (table.ViewExpandedTextHasBeenSet()) {
		result.view_expanded_text = ToStdString(table.GetViewExpandedText());
	}
	if (table.DescriptionHasBeenSet()) {
		result.description = ToStdString(table.GetDescription());
	}
	auto &storage_descriptor = table.GetStorageDescriptor();
	result.location = ToStdString(storage_descriptor.GetLocation());
	result.input_format = ToStdString(storage_descriptor.GetInputFormat());
	result.output_format = ToStdString(storage_descriptor.GetOutputFormat());
	result.serde_library = ToStdString(storage_descriptor.GetSerdeInfo().GetSerializationLibrary());
	result.serde_parameters = ToStdMap(storage_descriptor.GetSerdeInfo().GetParameters());
	result.columns = ToColumns(storage_descriptor.GetColumns());
	result.partition_keys = ToColumns(table.GetPartitionKeys());
	for (auto &column : storage_descriptor.GetBucketColumns()) {
		result.bucket_columns.push_back(ToStdString(column));
	}
	if (storage_descriptor.NumberOfBucketsHasBeenSet()) {
		result.number_of_buckets = storage_descriptor.GetNumberOfBuckets();
	}
	for (auto &column : storage_descriptor.GetSortColumns()) {
		GlueColumn sort_column;
		sort_column.name = ToStdString(column.GetColumn());
		// Glue's SortOrder is 1 for ascending, 0 for descending
		sort_column.sort_order = column.GetSortOrder() == 0 ? GlueSortOrder::DESCENDING : GlueSortOrder::ASCENDING;
		result.sort_columns.push_back(std::move(sort_column));
	}
	result.parameters = ToStdMap(table.GetParameters());
	return result;
}

string PartitionValuesToString(const vector<string> &values) {
	return StringUtil::Join(values, ", ");
}

Aws::Vector<Aws::String> ToAwsValues(const vector<string> &values) {
	return Aws::Vector<Aws::String>(values.begin(), values.end());
}

void CheckWritable(const GlueCatalog &catalog, const string &operation) {
	if (catalog.access_mode == AccessMode::READ_ONLY) {
		throw InvalidInputException("Cannot execute Glue %s on database %s which is attached in read-only mode!",
		                            operation, catalog.GetName());
	}
}

Aws::Map<Aws::String, Aws::String> ToAwsMap(const unordered_map<string, string> &input) {
	Aws::Map<Aws::String, Aws::String> result;
	for (auto &entry : input) {
		result.emplace(entry.first, entry.second);
	}
	return result;
}

Aws::Vector<Aws::Glue::Model::Column> ToAwsColumns(const vector<GlueColumn> &input) {
	Aws::Vector<Aws::Glue::Model::Column> result;
	for (auto &column : input) {
		Aws::Glue::Model::Column aws_column;
		aws_column.SetName(column.name);
		aws_column.SetType(column.type);
		if (!column.comment.empty()) {
			aws_column.SetComment(column.comment);
		}
		result.push_back(std::move(aws_column));
	}
	return result;
}

} // namespace duckdb
