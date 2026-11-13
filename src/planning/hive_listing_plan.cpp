#include "planning/hive_listing_plan.hpp"

#include "core/hive_location.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/common/unordered_set.hpp"

namespace duckdb {

namespace {

//! The keys S3 returns per ListObjectsV2 page
constexpr idx_t S3_KEYS_PER_LIST_PAGE = 1000;
//! At as many rounds, a listing replaces another only when it makes a tenth of its requests or fewer
constexpr idx_t REQUESTS_SAVED = 10;

struct ListingCost {
	idx_t rounds;
	idx_t requests;
};

idx_t PagesOf(idx_t keys) {
	return MaxValue<idx_t>((keys + S3_KEYS_PER_LIST_PAGE - 1) / S3_KEYS_PER_LIST_PAGE, 1);
}

//! Listings of one request each, 'at_a_time' at the same time
ListingCost CostOf(idx_t requests, idx_t at_a_time) {
	return ListingCost {(requests + at_a_time - 1) / at_a_time, requests};
}

//! A range's pages come one after another, and the ranges 'at_a_time' at the same time
ListingCost CostOf(const vector<HiveListingRange> &ranges, idx_t at_a_time) {
	idx_t pages = 0;
	idx_t longest = 0;
	for (auto &range : ranges) {
		pages += range.pages;
		longest = MaxValue(longest, range.pages);
	}
	return ListingCost {MaxValue(longest, CostOf(pages, at_a_time).rounds), pages};
}

bool Cheaper(const ListingCost &a, const ListingCost &b) {
	return a.rounds < b.rounds || (a.rounds == b.rounds && b.requests >= REQUESTS_SAVED * a.requests);
}

bool PrefixHasGlob(const string &prefix) {
	// DuckDB's glob also reads '\' as an escape
	return LocationHasGlob(prefix) || prefix.find('\\') != string::npos;
}

//! Splits the sorted partitions below the root into ranges that fit a listing page
struct RangeSplitter {
	const HiveListingPlanner &planner;
	const unordered_set<idx_t> &reading;
	vector<HiveListingRange> ranges;
	idx_t covered = 0;
	bool listable = true;

	//! [lo, hi) share their first 'depth' characters
	void Split(idx_t lo, idx_t hi, idx_t depth) {
		auto &entries = planner.registered_below_root;
		auto &first = entries[lo].first;
		auto &last = entries[hi - 1].first;
		while (depth < first.size() && depth < last.size() && first[depth] == last[depth]) {
			depth++;
		}
		auto keys = (hi - lo) * planner.keys_per_partition;
		// fits, or one partition's directory holds the rest
		if (keys <= S3_KEYS_PER_LIST_PAGE || first.size() == depth) {
			Emit(first.substr(0, depth), lo, hi, PagesOf(keys));
			return;
		}
		for (idx_t start = lo; start < hi && listable;) {
			auto c = entries[start].first[depth];
			idx_t end = start + 1;
			while (end < hi && entries[end].first[depth] == c) {
				end++;
			}
			Split(start, end, depth + 1);
			start = end;
		}
	}

	void Emit(const string &relative_prefix, idx_t lo, idx_t hi, idx_t pages) {
		vector<idx_t> read;
		for (idx_t i = lo; i < hi; i++) {
			if (reading.count(planner.registered_below_root[i].second)) {
				read.push_back(planner.registered_below_root[i].second);
			}
		}
		if (read.empty()) {
			return;
		}
		if (PrefixHasGlob(relative_prefix)) {
			listable = false;
			return;
		}
		covered += read.size();
		auto prefix = planner.root_prefix + relative_prefix;
		// a prefix ending inside a directory name covers the directories starting with it
		auto glob = StringUtil::EndsWith(prefix, "/") ? LocationGlob(prefix.substr(0, prefix.size() - 1))
		                                              : LocationGlob(prefix + "*");
		ranges.push_back({std::move(prefix), std::move(glob), std::move(read), pages});
	}
};

} // namespace

vector<HiveListingRange> HiveListingPlanner::PlanRanges(const vector<idx_t> &reading) const {
	if (registered_below_root.empty() || PrefixHasGlob(root_prefix)) {
		return {};
	}
	unordered_set<idx_t> reading_set(reading.begin(), reading.end());
	RangeSplitter splitter {*this, reading_set, {}};
	splitter.Split(0, registered_below_root.size(), 0);
	// a partition read that no range holds (its location spelled unlike the root) needs the other listings
	if (!splitter.listable || splitter.covered != reading_set.size()) {
		return {};
	}
	return std::move(splitter.ranges);
}

HiveListingPlan HiveListingPlanner::Plan(const vector<idx_t> &reading) const {
	auto root_pages = PagesOf(partitions_below_root * keys_per_partition);
	ListingCost root {root_pages, root_pages};
	auto partitions = CostOf(reading.size(), listed_together);
	bool list_root = threshold.IsValid() ? reading.size() >= threshold.GetIndex() : Cheaper(root, partitions);
	HiveListingPlan plan {list_root ? HiveListingMode::ROOT : HiveListingMode::PARTITIONS, {}};
	// with a threshold, ranges only replace the root; and no ranges beat a root of one page
	if (!ranges_enabled || root_pages <= 1 || (threshold.IsValid() && !list_root)) {
		return plan;
	}
	auto ranges = PlanRanges(reading);
	auto range_cost = CostOf(ranges, listed_together);
	// against one listing per partition, ranges must take fewer rounds: at as many, the small listings return sooner
	if (ranges.empty() || !(list_root ? Cheaper(range_cost, root) : range_cost.rounds < partitions.rounds)) {
		return plan;
	}
	plan.mode = HiveListingMode::RANGES;
	plan.ranges = std::move(ranges);
	return plan;
}

} // namespace duckdb
