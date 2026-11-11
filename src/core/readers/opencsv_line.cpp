#include "core/readers/opencsv_line.hpp"

#include "core/readers/hive_csv_options.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/multi_file/base_file_reader.hpp"
#include "duckdb/common/multi_file/multi_file_states.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression_iterator.hpp"

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

bool IsAllWhitespace(const string &text) {
	for (auto c : text) {
		if (!IsWhitespace(c)) {
			return false;
		}
	}
	return true;
}

//! As CsvDeserializer.parseLine: hands each of the first field_count fields to 'emit' (index, text), which returns
//! false to stop
template <class EMIT>
void ParseOpenCSVLine(const char *line, idx_t length, const OpenCSVDialect &dialect, idx_t field_count, string &field,
                      EMIT &&emit) {
	if (length == 0 || field_count == 0) {
		return;
	}
	field.clear();
	idx_t field_index = 0;
	bool in_quotes = false;
	bool in_field = false;
	// a position in UTF-16 code units, as Java counts the characters the embedded quote rule looks at
	idx_t char_position = 0;
	for (idx_t position = 0; position < length; position++) {
		auto start = position;
		auto c = line[position];
		auto has_next = position + 1 < length;
		if (c == dialect.escape) {
			if ((in_quotes || in_field) && has_next &&
			    (line[position + 1] == dialect.quote || line[position + 1] == dialect.escape)) {
				field += line[position + 1];
				position++;
			}
		} else if (c == dialect.quote) {
			if ((in_quotes || in_field) && has_next && line[position + 1] == dialect.quote) {
				field += line[position + 1];
				position++;
			} else {
				// a quote in the middle of a field, not next to a separator, is a literal character
				if (char_position > 2 && !IsSeparatorBefore(line, position, dialect.separator) && has_next &&
				    !IsSeparatorAt(line, length, position + 1, dialect.separator)) {
					if (!field.empty() && IsAllWhitespace(field)) {
						field.clear();
					} else {
						field += c;
					}
				}
				in_quotes = !in_quotes;
			}
			in_field = !in_field;
		} else if (!in_quotes && IsSeparatorAt(line, length, position, dialect.separator)) {
			position += dialect.separator.size() - 1;
			if (!emit(field_index, field)) {
				return;
			}
			field_index++;
			if (field_index == field_count) {
				return;
			}
			field.clear();
			in_field = false;
		} else {
			field += c;
			in_field = true;
		}
		for (auto consumed = start; consumed <= position; consumed++) {
			auto byte = static_cast<uint8_t>(line[consumed]);
			if ((byte & 0xC0) != 0x80) {
				// a character above the BMP is two UTF-16 code units
				char_position += byte >= 0xF0 ? 2 : 1;
			}
		}
	}
	if (!in_quotes) {
		emit(field_index, field);
	}
}

} // namespace

void SplitOpenCSVLine(const char *line, idx_t length, const OpenCSVDialect &dialect, vector<string> &fields,
                      vector<bool> &valid) {
	valid.assign(fields.size(), false);
	string field;
	ParseOpenCSVLine(line, length, dialect, fields.size(), field, [&](idx_t index, const string &text) {
		fields[index].assign(text);
		valid[index] = true;
		return true;
	});
}

bool GetOpenCSVLineField(const char *line, idx_t length, const OpenCSVDialect &dialect, idx_t index, string &field) {
	bool found = false;
	string text;
	ParseOpenCSVLine(line, length, dialect, index + 1, text, [&](idx_t field_index, const string &value) {
		if (field_index < index) {
			return true;
		}
		field.assign(value);
		found = true;
		return false;
	});
	return found;
}

void ReadOpenCSVLines(BaseFileReader &reader) {
	auto file_columns = reader.GetColumns().size();
	vector<bool> read(file_columns, false);
	for (idx_t i = 0; i < reader.column_ids.size(); i++) {
		auto file_column = reader.column_ids[MultiFileLocalIndex(i)].GetId();
		if (file_column < file_columns) {
			read[file_column] = true;
		}
	}
	for (idx_t file_column = 0; file_column < file_columns; file_column++) {
		if (!read[file_column]) {
			reader.column_ids.push_back(MultiFileLocalColumnId(file_column));
			reader.column_indexes.push_back(ColumnIndex(file_column));
		}
	}
}

void SplitOpenCSVLines(BaseFileReader &reader, const MultiFileReaderData &reader_data, DataChunk &chunk,
                       const HiveCSVOptions &options) {
	vector<bool> used(reader.column_ids.size(), false);
	for (auto &expr : reader_data.expressions) {
		if (!expr) {
			continue;
		}
		ExpressionIterator::VisitExpression<BoundReferenceExpression>(*expr, [&](const BoundReferenceExpression &ref) {
			if (ref.Index() < used.size()) {
				used[ref.Index()] = true;
			}
		});
	}
	auto file_columns = reader.GetColumns().size();
	vector<optional_idx> segments(file_columns);
	vector<pair<idx_t, idx_t>> targets;
	idx_t field_count = 0;
	for (idx_t i = 0; i < reader.column_ids.size(); i++) {
		auto file_column = reader.column_ids[MultiFileLocalIndex(i)].GetId();
		if (file_column >= file_columns) {
			continue;
		}
		if (!segments[file_column].IsValid()) {
			segments[file_column] = i;
		}
		if (used[i]) {
			targets.emplace_back(i, file_column);
			field_count = MaxValue<idx_t>(field_count, file_column + 1);
		}
	}
	for (auto &segment : segments) {
		if (!segment.IsValid()) {
			throw InternalException("Hive text table: a column of the lines of a line_fields file was not read");
		}
	}
	if (targets.empty()) {
		return;
	}
	auto count = chunk.size();
	OpenCSVDialect dialect {options.delimiter, options.quote[0], options.escape[0]};
	vector<Vector> fields_read;
	for (idx_t t = 0; t < targets.size(); t++) {
		fields_read.emplace_back(LogicalType::VARCHAR, count);
	}
	{
		vector<VectorWriter<string_t>> writers;
		for (auto &vector : fields_read) {
			writers.push_back(FlatVector::Writer<string_t>(vector, count));
		}
		vector<VectorIterator<string_t>> pieces;
		pieces.reserve(segments.size());
		for (auto &segment : segments) {
			pieces.push_back(chunk.data[segment.GetIndex()].Values<string_t>());
		}
		vector<string> fields(field_count);
		vector<bool> valid;
		string joined;
		for (idx_t row = 0; row < count; row++) {
			// read_csv splits a line at each NUL byte, the delimiter it is given, and pads the columns after its last
			// piece with NULL
			auto first = pieces[0][row];
			const char *line = nullptr;
			idx_t length = 0;
			if (first.IsValid()) {
				line = first.GetValue().GetData();
				length = first.GetValue().GetSize();
			}
			if (pieces.size() > 1 && pieces[1][row].IsValid()) {
				joined.assign(line, length);
				for (idx_t p = 1; p < pieces.size() && pieces[p][row].IsValid(); p++) {
					auto &piece = pieces[p][row].GetValue();
					joined += '\0';
					joined.append(piece.GetData(), piece.GetSize());
				}
				line = joined.data();
				length = joined.size();
			}
			if (first.IsValid()) {
				SplitOpenCSVLine(line, length, dialect, fields, valid);
			} else {
				valid.assign(field_count, false);
			}
			for (idx_t t = 0; t < targets.size(); t++) {
				auto file_column = targets[t].second;
				if (valid[file_column]) {
					auto &field = fields[file_column];
					writers[t].WriteValue(string_t(field.data(), UnsafeNumericCast<uint32_t>(field.size())));
				} else {
					writers[t].WriteNull();
				}
			}
		}
	}
	for (idx_t t = 0; t < targets.size(); t++) {
		chunk.data[targets[t].first].Reference(fields_read[t]);
	}
}

} // namespace duckdb
