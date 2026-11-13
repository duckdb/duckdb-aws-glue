#pragma once

#include "duckdb/common/open_file_info.hpp"
#include "duckdb/common/optional_idx.hpp"
#include "duckdb/common/path.hpp"
#include "duckdb/common/string.hpp"
#include "duckdb/common/unique_ptr.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {
class FileSystem;
class MultiFileList;

//! A location to compare with other locations: s3a://, s3n:// and S3:// name the same bucket and key as s3://, and a
//! trailing slash is dropped. Only for comparing; files are listed and read at the location as registered.
string ComparableLocation(const string &location);
//! Whether one comparable location holds the other, so that files of one are files of the other
bool LocationsOverlap(const string &a, const string &b);
//! The hive directory of a partition relative to its table: <key>=<value>/..., escaped, with a Glue value of
//! __HIVE_DEFAULT_PARTITION__ (NULL) kept as is
Path HivePartitionDirectory(const vector<string> &keys, const vector<string> &values);
//! The size the listing reported for a file, when it reported one. S3 and the local filesystem both carry it, so the
//! bytes of a table are known from the listing alone, without opening anything
optional_idx ListedFileSize(const OpenFileInfo &file);
//! Whether a listed file below 'prefix' is a data file of a Hive table: not hidden (a path component below 'prefix'
//! starting with '_' or '.', as Spark's _SUCCESS), not a directory marker (<name>_$folder$, as EMR and S3 tools write
//! next to a directory) and not empty
bool IsDataFile(const OpenFileInfo &file, const string &prefix);

//! Whether listing a location would read it as a glob pattern (*, ? or [), which misses its own files and matches
//! those of other prefixes
bool LocationHasGlob(const string &location);
//! Every file below a location, recursively; empty when there are none. Check LocationHasGlob first
unique_ptr<MultiFileList> ListLocation(FileSystem &fs, const string &location);

} // namespace duckdb
