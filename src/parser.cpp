#include "include/parser.h"
#include <iostream>

db_transfer::Column db_transfer::ParseColumnMeta(std::string json_str) {
	rapidjson::Document doc;
	doc.Parse(json_str.c_str());
	db_transfer::Column column;
	
	column.col_name_ = doc["Name"].GetString();
	column.ordinal_ = doc["Ordinal"].GetInt();
	column.unsign_ = doc["Unsigned"].GetBool();
	if (!doc["CharSet"].IsNull()) {
		column.charset_ = doc["CharSet"].GetString();
	}
	column.col_def_ = doc["ColumnDef"].GetString();
	if (doc["Length"].IsNull()) {
		column.len_ = -1;
	} else {
		column.len_ = doc["Length"].GetInt();
	}

	if (doc["Precision"].IsNull()) {
		column.precis_ = -1;
	} else {
		column.precis_ = doc["Precision"].GetInt();
	}

	if (doc["Scale"].IsNull()) {
		column.scale_ = -1;
	} else {
		column.scale_ = doc["Scale"].GetInt();
	}
	return column;
}

db_transfer::Index db_transfer::ParseIndexMeta(std::string json_str) {
	rapidjson::Document doc;
	doc.Parse(json_str.c_str());
	db_transfer::Index index(doc["IndexName"].GetString(), doc["Primary"].GetBool(), doc["Unique"].GetBool());
	for (int i = 0; i < doc["IndexCols"].GetArray().Size(); ++i) {
		index.index_cols_.emplace_back(doc["IndexCols"].GetArray()[i].GetString());
	}
	return index;
}

db_transfer::Table db_transfer::ParseTable(std::string multi_line_str) {
	std::stringstream ss;
	ss << multi_line_str;
	std::string str;
	int val;
	Table db_table;
	ss >> str;
	ss >> str;
	db_table.db_name_ = str;
	ss >> str;
	ss >> str;
	db_table.table_name_ = str;
	ss >> str;
	ss >> str;
	// Column number
	ss >> val;
	db_table.columns_.reserve(val);
	for (int i = 0; i < val; ++i) {
		ss >> str;
		Column col = ParseColumnMeta(str);
		db_table.columns_.emplace_back(col);
		db_table.name2col_.insert(std::make_pair(col.col_name_, i));
	}
	ss >> str;
	ss >> str;
	// Index number
	ss >> val;
	for (int i = 0; i < val; ++i) {
		ss >> str;
		Index index = ParseIndexMeta(str);
		db_table.indexs_.emplace_back(index);
		db_table.name2index_.insert(std::make_pair(index.index_name_, i));
	}
	// Since primary key records included in index records, jump them
	return db_table;
}

std::string db_transfer::OperationParser::GetTableNameFromOp(std::string op_record) {
	int idx = 0;
	idx = op_record.find('\t', idx) + 1;
	idx = op_record.find('\t', idx) + 1;
	int next_idx = idx;
	next_idx = op_record.find('\t', next_idx);
	return op_record.substr(idx, next_idx - idx);
}

void db_transfer::OperationParser::ProcessOp(std::string op_record) {
	switch(op_record[0]){
		case 'I':
			this->RecordInsert(op_record);
			break;
		case 'B':
			this->RecordPendingUpdate(op_record);
			break;
		case 'A':
			this->RecordUpdate(op_record);
			break;
		case 'D':
			this->RecordDelete(op_record);
			break;
		default:
			break;
	}
}

std::string db_transfer::VerifyValid(std::string val, std::string col_def, int extra_param_text_len) {
	if (col_def.find("datetime") != std::string::npos) {
		if (std::regex_match(val, std::regex("\\d{4}-\\d{2}-\\d{2}\\ \\d{2}:\\d{2}:\\d{2}\\.\\d{1}"))){
			return val;
		}
		return "2020-04-01 00:00:00.0";
	} else if (col_def.find("int") != std::string::npos) {
		if (!std::regex_match(val, std::regex("\\d+"))) {
			return "0";
		}
		int idx = 0;
		idx = col_def.find('(', idx) + 1;
		int n = std::stoi(col_def.substr(idx, col_def.size() - idx - 1));
		if (val.size() > n) {
			val = val.substr(val.size() - n);
		}
		return val;
	} else if (col_def.find("char") != std::string::npos) {
		int idx = 0;
		idx = col_def.find('(', idx) + 1;
		int n = std::stoi(col_def.substr(idx, col_def.size() - idx - 1));
		if (val.size() > n) {
			val = val.substr(0, n);
		}
		return val;
	} else if (col_def.find("text") != std::string::npos) {
		if (val.size() > extra_param_text_len) {
			val = val.substr(0, extra_param_text_len);
		}
		return val;
	} else {
		if (!std::regex_match(val, std::regex("-?\\d+\\.\\d+"))) {
			return "0";
		}
		int left_brac_idx = col_def.find('(', 0);
		int dot_idx = col_def.find(',', 0);
		int right_brac_idx = col_def.find(')', 0);
		int m = std::stoi(col_def.substr(left_brac_idx + 1, dot_idx - left_brac_idx - 1));
		int n = std::stoi(col_def.substr(dot_idx + 1, right_brac_idx - dot_idx - 1));
		int val_dot_idx = val.find('.');
		std::string int_part = val.substr(0, val_dot_idx);
		std::string fac_part = val.substr(val_dot_idx + 1);
		if (fac_part.size() > n) {
			if (fac_part[n] > '4') {
				int carray = 1;
				for (int i = n-1; i >=0 && carray == 1; --i) {
					if (fac_part[i] == '9') {
						fac_part[i] = '0';
					} else {
						carray = 0;
						fac_part[i] += 1;
					}
				}
				if (carray == 1) {
					for (int i = int_part.size() - 1; i >=0 && carray == 1; --i) {
						if (int_part[i] == '9') {
							int_part[i] = '0';
						} else {
							carray = 0;
							int_part[i] += 1;
						}
					}
					if (carray == 1) {
						int_part.insert(int_part.begin(), '1');
					}
				}
			}
			fac_part = fac_part.substr(0, n);
		}
		if (int_part.size() > (m - n)) {
			int_part = int_part.substr(int_part.size() - (m - n));
		}
		return int_part+"."+fac_part;
	}
}

std::vector<std::string> db_transfer::OperationParser::RowRecordsAsVec(std::string op_record) {
	int idx = 0;
	idx = op_record.find('\t', idx) + 1;
	idx = op_record.find('\t', idx) + 1;
	idx = op_record.find('\t', idx) + 1;
	// Table fields
	std::vector<std::string> row_record;
	int next_idx = idx;
	while ((next_idx = op_record.find('\t', idx)) != std::string::npos) {
		row_record.emplace_back(op_record.substr(idx, next_idx - idx));
		idx = next_idx + 1;
		next_idx++;
	}
	row_record.emplace_back(op_record.substr(idx));
	// // Verify fields and adjust
	// for (int i = 0; i < row_record.size(); ++i) {
	// 	row_record[i] = VerifyValid(row_record[i], table_meta_.columns_[i].col_def_);
	// }
	return row_record;
}

void db_transfer::OperationParser::RecordInsert(std::string op_record) {
	// The operation assigned to this parser is just only for this table
	std::vector<std::string> row_record = this->RowRecordsAsVec(op_record);
	// Insert pk2row map
	std::string pk_str;
	for (auto idx : this->primary_key_idxs) {
		pk_str += row_record[idx]+"|";
	}
	// Check primary key
	if (this->table_data_.pk2row.find(pk_str) != this->table_data_.pk2row.end()) {
		// Update with newest record
		*(this->table_data_.pk2row[pk_str]) = row_record;
		return;
	}

	this->table_data_.datas_.emplace_front(std::move(TableRow(row_record)));
	this->table_data_.pk2row.insert(std::make_pair(pk_str, this->table_data_.datas_.begin()));
}

std::string db_transfer::OperationParser::GetPKFromSrcRowRecord(std::string row_record) {
	std::vector<std::string> row_vec = this->RowRecordsAsVec(row_record);
	std::string pk_str;
	for (auto idx : this->primary_key_idxs) {
		pk_str += row_vec[idx]+"|";
	}
	return pk_str;
}

void db_transfer::OperationParser::RecordPendingUpdate(std::string op_record) {
	std::vector<std::string> row_record = this->RowRecordsAsVec(op_record);
	std::string pk_str;
	for (auto idx : this->primary_key_idxs) {
		pk_str += row_record[idx] + "|";
	}
	this->table_data_.update_before_queue_.push(pk_str);
}

void db_transfer::OperationParser::RecordUpdate(std::string op_record) {
	if (this->table_data_.update_before_queue_.empty()) {
		return;
	}
	std::vector<std::string> row_records = this->RowRecordsAsVec(op_record);
	std::string pk_str = this->table_data_.update_before_queue_.front();
	this->table_data_.update_before_queue_.pop();
	// Find position of the `B` operation
	std::list<TableRow>::iterator record_pos = this->table_data_.pk2row[pk_str];
	// Update
	record_pos->fields_ = row_records;
	// Remove old primary key and replace with new one
	this->table_data_.pk2row.erase(pk_str);
	pk_str = "";
	for (auto idx : this->primary_key_idxs) {
		pk_str += row_records[idx] + "|";
	}
	this->table_data_.pk2row.insert(std::make_pair(pk_str, record_pos));
}

void db_transfer::OperationParser::RecordDelete(std::string op_record) {
	std::vector<std::string> row_record = this->RowRecordsAsVec(op_record);
	std::string pk_str;
	for (auto idx : this->primary_key_idxs) {
		pk_str += row_record[idx] + "|";
	}
	std::list<TableRow>::iterator record_pos = this->table_data_.pk2row[pk_str];
	this->table_data_.datas_.erase(record_pos);
	this->table_data_.pk2row.erase(pk_str);
}