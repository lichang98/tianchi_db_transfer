#pragma once
// == Parser for parse records read from schema.info and data load from src data
#include "include/table_handler.h"

#include "include/rapidjson/document.h"
#include "include/rapidjson/writer.h"
#include "include/rapidjson/stringbuffer.h"

#include <iostream>
#include <sstream>

namespace db_transfer
{
	Column ParseColumnMeta(std::string json_str);
	Index ParseIndexMeta(std::string json_str);
	Table ParseTable(std::string multi_line_str);

	// == Parse operations and apply to table entity
	// Each `OperationParser` processes operations according one table
	class OperationParser
	{
	public:
		void ProcessOp(std::string op_record);
		void RecordInsert(std::string op_record);
		void RecordPendingUpdate(std::string op_record);
		void RecordUpdate(std::string op_record);
		void RecordDelete(std::string op_record);

		std::vector<std::string> RowRecordsAsVec(std::string op_record);


		OperationParser() {
			for (auto index : table_meta_.indexs_) {
				if (index.primary_) {
					for (auto col_name : index.index_cols_) {
						primary_key_idxs.emplace_back(table_meta_.name2index_[col_name]);
					}
				}
			}
		}

		OperationParser(Table &table_meta) {
			this->table_meta_ = table_meta;
			for (auto index : table_meta_.indexs_) {
				if (index.primary_) {
					for (auto col_name : index.index_cols_) {
						primary_key_idxs.emplace_back(table_meta_.name2index_[col_name]);
					}
				}
			}
		}

		// The array index of primary key
		std::vector<int> primary_key_idxs;
		Table table_meta_;
		TableData table_data_;
	};
};

