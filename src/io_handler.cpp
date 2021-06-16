#include "include/io_handler.h"


std::string db_transfer::IOHandler::LoadSchema(const std::string schema_fpath, bool &data_avail) {
	char *line = new char[1024];
	if (!this->schema_fs_.is_open()) {
		this->schema_fs_.open(schema_fpath, std::fstream::in);
		this->schema_fs_.getline(line, 1024);
		std::string line_str(line);
		this->ss_ << line_str << "\n";
	} else if(this->schema_fs_.eof()) {
		data_avail = false;
		this->schema_fs_.close();
		delete[] line;
		return "";
	}
	do {
		this->schema_fs_.getline(line, 1024);
		std::string line_str(line);
		if (line_str.length() > 10 && line_str.substr(0, 10) == "[DATABASE]") {
			break;
		}
		this->ss_ << line_str << "\n";
	} while (!this->schema_fs_.eof());
	std::string schme_data = this->ss_.str();
	this->ss_.str("");
	this->ss_ << std::string(line) << "\n";
	delete[] line;
	return schme_data;
}

std::string db_transfer::IOHandler::LoadSrcDataNextLine(const std::string src_fpath, bool &data_avail, std::string &table_name) {
	char *line = new char[1024];
	if (!this->src_data_fs_.is_open()) {
		this->src_data_fs_.open(src_fpath, std::fstream::in);
	} else if (this->src_data_fs_.eof()) {
		data_avail = false;
		this->src_data_fs_.close();
		delete[] line;
		return "";
	}

	this->src_data_fs_.getline(line, 1024);
	std::string line_str(line);
	int idx_tab1 = 0;
	idx_tab1 = line_str.find('\t', idx_tab1) + 1;
	idx_tab1 = line_str.find('\t', idx_tab1) + 1;
	int idx_tab2 = idx_tab1;
	idx_tab2 = line_str.find('\t', idx_tab2);
	table_name = line_str.substr(idx_tab1, idx_tab2 - idx_tab1);
	delete[] line;
	return line_str;
}

int64_t db_transfer::IOHandler::GetCurrPosition() {
	return this->src_data_fs_.tellg();
}

void db_transfer::IOHandler::SaveTableData(TableData table_data, std::string save_file_path) {
	std::fstream fs;
	fs.open(save_file_path, std::fstream::out);
	for (auto &row : table_data.datas_) {
		fs << row << "\n";
	}
	fs.close();
}