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
		db_table.columns_.insert(std::make_pair(col.col_name_, std::move(col)));
	}
	ss >> str;
	ss >> str;
	// Index number
	ss >> val;
	for (int i = 0; i < val; ++i) {
		ss >> str;
		Index index = ParseIndexMeta(str);
		db_table.indexs_.emplace_back(std::move(index));
	}
	// Since primary key records included in index records, jump them
	return db_table;
}