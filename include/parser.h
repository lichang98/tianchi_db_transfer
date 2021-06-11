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
};

