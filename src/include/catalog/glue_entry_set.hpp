#pragma once

#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Identifies an entry set (GlueTableSet / GlueSchemaSet) among the sets a GlueTransactionCache has listed
using glue_entry_set_id_t = idx_t;

//! The names in 'entries' that a listing no longer returned
template <class ENTRY>
vector<string> UnlistedNames(const case_insensitive_map_t<ENTRY> &entries, const case_insensitive_set_t &listed) {
	vector<string> result;
	for (auto &entry : entries) {
		if (!listed.count(entry.first)) {
			result.push_back(entry.first);
		}
	}
	return result;
}

} // namespace duckdb
