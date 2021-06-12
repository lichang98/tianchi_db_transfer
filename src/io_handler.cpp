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