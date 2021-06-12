#pragma once
// == Parser for parse records read from schema.info and data load from src data
#include "include/io_handler.h"

#include "include/rapidjson/document.h"
#include "include/rapidjson/writer.h"
#include "include/rapidjson/stringbuffer.h"

#include <iostream>
#include <sstream>
#include <algorithm>

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
				} else if (index.unique_) {
					for (auto col_name : index.index_cols_) {
						unique_key_idxs.emplace_back(table_meta_.name2index_[col_name]);
					}
				}
			}
			// Sort primary key idxs
			std::sort(primary_key_idxs.begin(), primary_key_idxs.end());
			// Sort unique key idxs
			std::sort(unique_key_idxs.begin(), unique_key_idxs.end());
		}

		// The array index of primary key
		std::vector<int> primary_key_idxs;
		// Unique keys (not include primary key)
		std::vector<int> unique_key_idxs;
		Table table_meta_;
		TableData table_data_;
	};
};

