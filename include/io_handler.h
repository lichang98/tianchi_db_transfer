#pragma once

#include "table_handler.h"

#include "include/rapidjson/document.h"
#include "include/rapidjson/writer.h"
#include "include/rapidjson/stringbuffer.h"

#include <iostream>
#include <fstream>
#include <sstream>

namespace db_transfer {
    
    class IOHandler 
    {
    public:
			IOHandler() {
			}

			// Each time load and return schema info of one table
			std::string LoadSchema(const std::string schema_fpath, bool &data_avail);
			// Each time load one line from src data file
			std::string LoadSrcDataNextLine(const std::string src_fpath, bool &data_avail, std::string &table_name);
			// After loading and processing src data, save it to file by row
			void SaveTableData(TableData table_data, std::string save_file_path);

			std::stringstream ss_;
			std::fstream schema_fs_;
			std::fstream src_data_fs_;
			std::fstream out_data_fs_;
    };
};