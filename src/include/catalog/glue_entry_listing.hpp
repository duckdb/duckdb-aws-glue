#pragma once

#include "duckdb/catalog/catalog_entry.hpp"
#include "duckdb/common/case_insensitive_map.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! Helpers for merging a Glue listing into an entry set (GlueTableSet, GlueSchemaSet) whose Glue call ran without the
//! set's lock. The entries as they were when the call started tell which names another client changed meanwhile:
//! those carry fresher information than the listing and are left alone.
struct GlueEntryListing {
	//! name -> the entry object the set held when the listing started (null for none)
	using Snapshot = case_insensitive_map_t<CatalogEntry *>;

	template <class ENTRY>
	static Snapshot Take(const case_insensitive_map_t<unique_ptr<ENTRY>> &entries) {
		Snapshot snapshot;
		for (auto &entry : entries) {
			snapshot.emplace(entry.first, entry.second.get());
		}
		return snapshot;
	}

	//! Whether 'name' holds the entry it held when the snapshot was taken (none then and none now included)
	template <class ENTRY>
	static bool Unchanged(const case_insensitive_map_t<unique_ptr<ENTRY>> &entries, const Snapshot &snapshot,
	                      const string &name) {
		auto then = snapshot.find(name);
		auto now = entries.find(name);
		CatalogEntry *before = then == snapshot.end() ? nullptr : then->second;
		CatalogEntry *after = now == entries.end() ? nullptr : now->second.get();
		return before == after;
	}

	//! The names the listing did not return that still hold the entry they held when it started: dropped by another
	//! client, to be retired
	template <class ENTRY>
	static vector<string> NotListed(const case_insensitive_map_t<unique_ptr<ENTRY>> &entries, const Snapshot &snapshot,
	                                const case_insensitive_set_t &listed) {
		vector<string> result;
		for (auto &entry : entries) {
			if (!listed.count(entry.first) && Unchanged(entries, snapshot, entry.first)) {
				result.push_back(entry.first);
			}
		}
		return result;
	}
};

} // namespace duckdb
