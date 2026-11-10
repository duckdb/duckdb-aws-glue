#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/execution/operator/persistent/physical_copy_to_file.hpp"
#include "duckdb/planner/parsed_data/bound_create_table_info.hpp"

#include "core/glue_info.hpp"

namespace duckdb {
class GlueCatalog;
class GlueSchemaEntry;

//! The COPY of INSERT and CTAS into a Hive table; CTAS creates the Glue table when the sink starts, like
//! PhysicalInsert. The write is added to the Glue transaction before anything is written, so that a failed statement
//! can undo it.
class GlueHiveCopy final : public PhysicalCopyToFile {
public:
	//! INSERT: no schema and info. CTAS: the schema to create in, the bound statement and the definition resolved at
	//! planning, which the create uses as is. The files are named <write_id>_..., below the table's location.
	GlueHiveCopy(PhysicalPlan &physical_plan, vector<LogicalType> types, CopyFunction function,
	             unique_ptr<FunctionData> bind_data, idx_t estimated_cardinality, GlueCatalog &catalog,
	             optional_ptr<GlueSchemaEntry> schema, unique_ptr<BoundCreateTableInfo> info, GlueTableInfo table_info,
	             const string &write_id);

	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const override;
	string GetName() const override;

private:
	GlueCatalog &catalog;
	optional_ptr<GlueSchemaEntry> schema;
	unique_ptr<BoundCreateTableInfo> info;
	GlueTableInfo table_info;
	string write_id;
	mutable annotated_mutex start_lock;
};

} // namespace duckdb
