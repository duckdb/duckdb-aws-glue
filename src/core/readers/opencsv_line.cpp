#include "core/readers/opencsv_line.hpp"

#include <cstring>

namespace duckdb {

namespace {

//! Java's Character.isWhitespace for ASCII
bool IsWhitespace(char c) {
	return c == ' ' || (c >= '\t' && c <= '\r') || (c >= 0x1C && c <= 0x1F);
}

bool IsSeparatorAt(const char *line, idx_t length, idx_t position, const string &separator) {
	return position + separator.size() <= length && memcmp(line + position, separator.data(), separator.size()) == 0;
}

bool IsSeparatorBefore(const char *line, idx_t position, const string &separator) {
	return position >= separator.size() &&
	       memcmp(line + position - separator.size(), separator.data(), separator.size()) == 0;
}

//! As CsvDeserializer.parseLine: hands each of the first field_count fields that 'want' (index) asks for to 'emit'
//! (index, text), which returns false to stop; the fields it does not ask for are parsed but not kept
template <class WANT, class EMIT>
void ParseOpenCSVLine(const char *line, idx_t length, const OpenCSVDialect &dialect, idx_t field_count, string &field,
                      WANT &&want, EMIT &&emit) {
	if (length == 0 || field_count == 0) {
		return;
	}
	auto separator_start = dialect.separator[0];
	auto separator_size = dialect.separator.size();
	field.clear();
	idx_t field_index = 0;
	bool keep = want(field_index);
	// whether the field holds text, and only whitespace, kept or not
	bool field_empty = true;
	bool field_blank = true;
	bool in_quotes = false;
	bool in_field = false;
	// a position in UTF-16 code units, as Java counts the characters the embedded quote rule looks at; only whether it
	// is past 2 matters, so the count stops there
	idx_t char_position = 0;
	auto append = [&](const char *text, idx_t size) {
		if (keep) {
			field.append(text, size);
		}
		field_empty = false;
		for (idx_t i = 0; field_blank && i < size; i++) {
			field_blank = IsWhitespace(text[i]);
		}
	};
	auto count = [&](idx_t start, idx_t end) {
		for (auto consumed = start; consumed < end && char_position <= 2; consumed++) {
			auto byte = static_cast<uint8_t>(line[consumed]);
			if ((byte & 0xC0) != 0x80) {
				// a character above the BMP is two UTF-16 code units
				char_position += byte >= 0xF0 ? 2 : 1;
			}
		}
	};
	auto special = [&](char c) {
		return c == dialect.escape || c == dialect.quote || c == separator_start;
	};
	idx_t position = 0;
	while (position < length) {
		auto c = line[position];
		if (!special(c)) {
			auto end = position + 1;
			while (end < length && !special(line[end])) {
				end++;
			}
			append(line + position, end - position);
			in_field = true;
			count(position, end);
			position = end;
			continue;
		}
		auto start = position;
		auto has_next = position + 1 < length;
		if (c == dialect.escape) {
			if ((in_quotes || in_field) && has_next &&
			    (line[position + 1] == dialect.quote || line[position + 1] == dialect.escape)) {
				append(line + position + 1, 1);
				position++;
			}
		} else if (c == dialect.quote) {
			if ((in_quotes || in_field) && has_next && line[position + 1] == dialect.quote) {
				append(line + position + 1, 1);
				position++;
			} else {
				// a quote in the middle of a field, not next to a separator, is a literal character
				if (char_position > 2 && !IsSeparatorBefore(line, position, dialect.separator) && has_next &&
				    !IsSeparatorAt(line, length, position + 1, dialect.separator)) {
					if (!field_empty && field_blank) {
						field.clear();
						field_empty = true;
					} else {
						append(&c, 1);
					}
				}
				in_quotes = !in_quotes;
			}
			in_field = !in_field;
		} else if (!in_quotes && IsSeparatorAt(line, length, position, dialect.separator)) {
			position += separator_size - 1;
			if (keep && !emit(field_index, field)) {
				return;
			}
			field_index++;
			if (field_index == field_count) {
				return;
			}
			field.clear();
			field_empty = true;
			field_blank = true;
			keep = want(field_index);
			in_field = false;
		} else {
			append(&c, 1);
			in_field = true;
		}
		count(start, position + 1);
		position++;
	}
	if (!in_quotes && keep) {
		emit(field_index, field);
	}
}

} // namespace

void SplitOpenCSVLine(const char *line, idx_t length, const OpenCSVDialect &dialect, const vector<bool> &wanted,
                      vector<string> &fields, vector<bool> &valid, string &buffer) {
	valid.assign(fields.size(), false);
	ParseOpenCSVLine(
	    line, length, dialect, fields.size(), buffer, [&](idx_t index) { return bool(wanted[index]); },
	    [&](idx_t index, const string &text) {
		    fields[index].assign(text);
		    valid[index] = true;
		    return true;
	    });
}

bool GetOpenCSVLineField(const char *line, idx_t length, const OpenCSVDialect &dialect, idx_t index, string &field) {
	bool found = false;
	string text;
	ParseOpenCSVLine(
	    line, length, dialect, index + 1, text, [&](idx_t field_index) { return field_index == index; },
	    [&](idx_t, const string &value) {
		    field.assign(value);
		    found = true;
		    return false;
	    });
	return found;
}

} // namespace duckdb
