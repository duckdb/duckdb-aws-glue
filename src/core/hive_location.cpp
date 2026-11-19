#include "core/hive_location.hpp"

#include "duckdb/common/file_system.hpp"
#include "duckdb/common/multi_file/multi_file_list.hpp"
#include "duckdb/common/hive_partitioning.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

string NormalizeLocation(const string &location) {
	auto result = location;
	StringUtil::RTrim(result, "/");
	auto scheme = StringUtil::Lower(result.substr(0, 6));
	if (scheme == "s3a://" || scheme == "s3n://") {
		result = "s3://" + result.substr(6);
	} else if (StringUtil::Lower(result.substr(0, 5)) == "s3://") {
		result = "s3://" + result.substr(5);
	}
	return result;
}

bool LocationsOverlap(const string &a, const string &b) {
	return a == b || StringUtil::StartsWith(a, b + "/") || StringUtil::StartsWith(b, a + "/");
}

bool LocationHasGlob(const string &location) {
	return FileSystem::HasGlob(location);
}

unique_ptr<MultiFileList> ListLocation(FileSystem &fs, const string &location) {
	return fs.GlobFileList(location + "/**", FileGlobOptions::ALLOW_EMPTY);
}

optional_idx ListedFileSize(const OpenFileInfo &file) {
	if (!file.extended_info) {
		return optional_idx();
	}
	idx_t size;
	if (!file.extended_info->TryGetOption("file_size", size)) {
		return optional_idx();
	}
	return size;
}

//! Whether a listed object below 'prefix' (the location, with its trailing slash) is a data file. Hive skips _* and .*
//! files and directories; EMR and S3 tools mark a directory with a <name>_$folder$ object next to it (an object, never
//! a directory, so only the last component is checked); and a zero-byte object of any name holds no rows and is not a
//! file of any format the reader opens
bool IsDataFile(const OpenFileInfo &file, const string &prefix) {
	if (!StringUtil::StartsWith(file.path, prefix)) {
		return false;
	}
	auto components = StringUtil::Split(file.path.substr(prefix.size()), '/');
	for (auto &component : components) {
		if (component.empty() || component[0] == '_' || component[0] == '.') {
			return false;
		}
	}
	if (components.empty() || StringUtil::EndsWith(components.back(), "_$folder$")) {
		return false;
	}
	auto size = ListedFileSize(file);
	return !size.IsValid() || size.GetIndex() > 0;
}

Path HivePartitionDirectory(const vector<string> &keys, const vector<string> &values) {
	D_ASSERT(keys.size() == values.size());
	Path result;
	for (idx_t i = 0; i < keys.size(); i++) {
		auto value = values[i] == HivePartitioning::DEFAULT_PARTITION_NAME ? values[i]
		                                                                   : HivePartitioning::EscapeValue(values[i]);
		result = result.Join(HivePartitioning::Escape(keys[i]) + "=" + value);
	}
	return result;
}

} // namespace duckdb
