#pragma once

#include <string>
#include <vector>
#include <unordered_map>

namespace db_transfer
{
	class Column 
	{
	public:
		// == Meta fields of a column
		std::string col_name_;
		int32_t ordinal_;
		bool unsign_;
		std::string charset_;
		std::string col_def_;
		// Only for varchar like type, if this field is null, -1
		int32_t len_;
		// Only for digit type, otherwise -1
		int32_t precis_;
		// Only for decimal, otherwise -1
		int32_t scale_;

		Column() {}
		Column(std::string col_name, int32_t ordinal, bool unsign,
					std::string charset, std::string col_def, int32_t len,
					int32_t precis, int32_t scale): col_name_(col_name),
					ordinal_(ordinal), unsign_(unsign),charset_(charset),
					col_def_(col_def), len_(len), precis_(precis), scale_(scale) {}
	};

	class Index
	{
	public:
		// == Index fields, used by common and primary key index
		std::string index_name_;
		std::vector<std::string> index_cols_;
		bool primary_;
		bool unique_;

		Index() {}
		Index(std::string index_name, bool primary, bool unique):index_name_(index_name),
					primary_(primary), unique_(unique) {}
		Index(std::string index_name, std::vector<std::string> index_cols,
				bool primary, bool unique): index_name_(index_name), index_cols_(index_cols),
				primary_(primary), unique_(unique) {}
	};

	class Table
	{
	public:
		std::string db_name_;
		std::string table_name_;
		// Map from column name to Column entity
		std::unordered_map<std::string, Column> columns_;
		// Array of indexs
		std::vector<Index> indexs_;

		Table() {}
		Table(std::string db_name, std::string table_name) {}
	};
} // namespace db_transfer
