#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! An integer from 'min' to 'max', false when 'value' is not one
bool TryParseInteger(const string &value, int64_t min, int64_t max, int64_t &result);
//! The first character of a string, false when it is empty or not valid UTF-8
bool TryFirstCharacter(const string &value, string &result);

//! Split a Glue catalog id into the account it leads with and the catalog below that account's Data Catalog. Either
//! can be empty: '<account>' is the Data Catalog itself, '<account>:<catalog>[/<child>]' a catalog below it, and
//! '<catalog>[/<child>]' one below the Data Catalog of the current account.
void SplitGlueCatalogId(const string &catalog_id, string &account, string &catalog);
//! Whether two Glue catalog ids name the same catalog, in the same account when both name one. An id without an
//! account matches the same catalog of any account.
bool IsSameGlueCatalogId(const string &left, const string &right);

} // namespace duckdb
