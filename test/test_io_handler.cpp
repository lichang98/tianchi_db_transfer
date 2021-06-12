#include "include/io_handler.h"
#include "include/parser.h"

#include <gtest/gtest.h>

TEST(IO_HANDLER, LOAD_SCHEMA) {
	const char *schme_file = "./demo-test/schema_info_dir/schema.info";
	db_transfer::IOHandler io_handler;
	bool is_avail = true;
	std::string one_table_schema = io_handler.LoadSchema(schme_file,is_avail);
	ASSERT_TRUE(is_avail);
	db_transfer::Table table = db_transfer::ParseTable(one_table_schema);
	ASSERT_EQ(table.db_name_, "tianchi_dts_data");
	ASSERT_EQ(table.table_name_, "item");
	ASSERT_EQ(table.columns_.size(), 5);
	ASSERT_EQ(table.indexs_.size(), 1);

	// Load schema of second table
	one_table_schema = io_handler.LoadSchema(schme_file, is_avail);
	ASSERT_TRUE(is_avail);
	table = db_transfer::ParseTable(one_table_schema);
	ASSERT_EQ(table.db_name_, "tianchi_dts_data");
	ASSERT_EQ(table.table_name_, "district");
	ASSERT_EQ(table.columns_.size(), 11);
	ASSERT_EQ(table.indexs_.size(), 2);

	for (int i = 0; i < 6; ++i) {
		one_table_schema = io_handler.LoadSchema(schme_file, is_avail);
		ASSERT_TRUE(is_avail);
		table = db_transfer::ParseTable(one_table_schema);
		ASSERT_EQ(table.db_name_, "tianchi_dts_data");
	}

	ASSERT_EQ(table.table_name_, "customer");
	ASSERT_EQ(table.columns_.size(), 21);
	ASSERT_EQ(table.indexs_.size(), 7);

	one_table_schema = io_handler.LoadSchema(schme_file, is_avail);
	ASSERT_FALSE(is_avail);
}

TEST(IO_HANDLER, LOAD_SRC) {
	const char *src_file = "./demo-test/source_file_dir/source_file_dir/tianchi_dts_source_data_1";
	db_transfer::IOHandler io_handler;
	std::string table_name;
	bool is_avail = true;
	std::string line_str = io_handler.LoadSrcDataNextLine(src_file, is_avail, table_name);
	ASSERT_TRUE(is_avail);
	ASSERT_EQ(table_name, "orders");
	ASSERT_EQ(line_str, "I	tianchi_dts_data	orders	5	1	1	1517	2021-04-11 15:41:55.0	9	6	1");
}