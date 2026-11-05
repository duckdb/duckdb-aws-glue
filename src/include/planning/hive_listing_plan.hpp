#pragma once

#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

//! How a listing job lists the partitions it reads
enum class HiveListingMode : uint8_t {
	//! The table location once, recursively: one page after another
	ROOT,
	//! Prefix ranges of the partitions below the table location, all at the same time
	RANGES,
	//! Each partition's location, several at the same time
	PARTITIONS
};

//! A contiguous run of the partitions below the table location that share a key prefix, listed with one prefix listing
struct HiveListingRange {
	//! The key prefix the range lists, e.g. <root>/part=24
	string prefix;
	//! The glob of its listing, e.g. <root>/part=24*/**
	string glob;
	//! The partitions of the range the scan reads
	vector<idx_t> partitions;
	//! The listing pages it takes, one after another
	idx_t pages = 1;
};

struct HiveListingPlan {
	HiveListingMode mode;
	//! The ranges to list, for RANGES
	vector<HiveListingRange> ranges;
};

//! Chooses the listing of the partitions read below a table location that takes the fewest rounds of requests
struct HiveListingPlanner {
	//! The table location with a trailing '/'
	string root_prefix;
	//! Every partition registered below root_prefix, by its location relative to it with a trailing '/', sorted
	vector<pair<string, idx_t>> registered_below_root;
	//! The partitions whose keys the root's listing holds
	idx_t partitions_below_root = 0;
	idx_t keys_per_partition = 1;
	//! How many listings run at the same time
	idx_t listed_together = 1;
	//! hive_partition_listing_threshold: the partitions read from which the root is listed rather than each of them
	optional_idx threshold;
	//! hive_partition_listing_ranges
	bool ranges_enabled = true;

	//! How to list 'reading', partitions below the root
	HiveListingPlan Plan(const vector<idx_t> &reading) const;
	//! Ranges of about a page holding 'reading'; none when they can not cover it (glob characters, another spelling)
	vector<HiveListingRange> PlanRanges(const vector<idx_t> &reading) const;
};

} // namespace duckdb
