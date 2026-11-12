#pragma once

#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/common/table_column.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/parser/column_list.hpp"

namespace duckdb {
class GlueTable;
class LogicalInsert;
class PhysicalOperator;
class PhysicalPlanGenerator;

//! A table's schema and supported operations, independent of its catalog identity.
class GlueTableImplementation {
public:
	explicit GlueTableImplementation(ColumnList columns) : columns(std::move(columns)) {
	}
	virtual ~GlueTableImplementation() = default;

	virtual const ColumnList &GetColumns() const {
		return columns;
	}
	virtual TableFunction BindScan(ClientContext &context, GlueTable &table, unique_ptr<FunctionData> &bind_data,
	                               const EntryLookupInfo &lookup) = 0;
	virtual PhysicalOperator &PlanInsert(ClientContext &context, GlueTable &table, PhysicalPlanGenerator &planner,
	                                     LogicalInsert &op, optional_ptr<PhysicalOperator> plan);
	virtual virtual_column_map_t GetVirtualColumns() const {
		return {};
	}
	virtual vector<column_t> GetRowIdColumns() const {
		return {};
	}

	static unique_ptr<GlueTableImplementation> CreateHive(const ColumnList &columns);
	static unique_ptr<GlueTableImplementation> CreateUnsupported(const ColumnList &columns);

private:
	ColumnList columns;
};

} // namespace duckdb
