#pragma once

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

			std::stringstream ss_;
			std::fstream schema_fs_;
			std::fstream src_data_fs_;
			std::fstream out_data_fs_;
    };
};