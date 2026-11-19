#include "core/helpers.hpp"

#include "duckdb/common/string_util.hpp"
#include "duckdb/common/types/value.hpp"

#include "utf8proc_wrapper.hpp"

namespace duckdb {

bool TryParseInteger(const string &value, int64_t min, int64_t max, int64_t &result) {
	if (value.empty()) {
		return false;
	}
	auto cast = Value(value).DefaultTryCastAs(LogicalType::BIGINT);
	if (!cast) {
		return false;
	}
	result = cast->GetValue<int64_t>();
	return result >= min && result <= max;
}

void SplitGlueCatalogId(const string &catalog_id, string &account, string &catalog) {
	constexpr idx_t account_id_digits = 12;
	account.clear();
	catalog = catalog_id;
	if (catalog_id.size() < account_id_digits) {
		return;
	}
	for (idx_t i = 0; i < account_id_digits; i++) {
		if (!StringUtil::CharacterIsDigit(catalog_id[i])) {
			return;
		}
	}
	if (catalog_id.size() == account_id_digits) {
		account = catalog_id;
		catalog.clear();
	} else if (catalog_id[account_id_digits] == ':') {
		account = catalog_id.substr(0, account_id_digits);
		catalog = catalog_id.substr(account_id_digits + 1);
	}
}

bool IsSameGlueCatalogId(const string &left, const string &right) {
	string left_account, left_catalog, right_account, right_catalog;
	SplitGlueCatalogId(left, left_account, left_catalog);
	SplitGlueCatalogId(right, right_account, right_catalog);
	if (!left_account.empty() && !right_account.empty() && left_account != right_account) {
		return false;
	}
	return StringUtil::CIEquals(left_catalog, right_catalog);
}

bool TryFirstCharacter(const string &value, string &result) {
	if (value.empty() || !Utf8Proc::IsValid(value.c_str(), value.size())) {
		return false;
	}
	int length;
	Utf8Proc::UTF8ToCodepoint(value.c_str(), length, value.size());
	result = value.substr(0, NumericCast<idx_t>(length));
	return true;
}

} // namespace duckdb
