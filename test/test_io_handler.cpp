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
	for (int i = 0; i < 62341; ++i) {
		line_str = io_handler.LoadSrcDataNextLine(src_file, is_avail, table_name);
		ASSERT_TRUE(is_avail);
	}
	line_str = io_handler.LoadSrcDataNextLine(src_file, is_avail, table_name);
	ASSERT_FALSE(is_avail);
}

TEST(IO_HANDLER, SAVE_TABLE_ROWS) {
	const char* table_meta = "[DATABASE] tianchi_dts_data [TABLE] item\n\
COLUMN NUMBER 5\n\
{\"Name\":\"i_id\",\"Ordinal\":1,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"int(11)\",\"Length\":null,\"Precision\":10,\"Scale\":0}\n\
{\"Name\":\"i_im_id\",\"Ordinal\":2,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"int(11)\",\"Length\":null,\"Precision\":10,\"Scale\":0}\n\
{\"Name\":\"i_name\",\"Ordinal\":3,\"Unsigned\":false,\"CharSet\":\"latin1\",\"ColumnDef\":\"varchar(24)\",\"Length\":24,\"Precision\":null,\"Scale\":null}\n\
{\"Name\":\"i_price\",\"Ordinal\":4,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"decimal(5,2)\",\"Length\":null,\"Precision\":5,\"Scale\":2}\n\
{\"Name\":\"i_data\",\"Ordinal\":5,\"Unsigned\":false,\"CharSet\":\"latin1\",\"ColumnDef\":\"varchar(50)\",\"Length\":50,\"Precision\":null,\"Scale\":null}\n\
INDEX NUMBER 1\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"i_id\"],\"Primary\":true,\"Unique\":true}\n\
PRIMARY KEY NUMBER 1\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"i_id\"],\"Primary\":true,\"Unique\":true}";

	db_transfer::Table db_table = db_transfer::ParseTable(table_meta);
	db_transfer::OperationParser op_parser(db_table);
	ASSERT_EQ(op_parser.primary_key_idxs.size(), 1);

	// Insert
	const char* rec_insert = "I	tianchi_dts_data	item	66	6858	4xl4nqtG6Ij1Og3G	8.46	Y52pyRj6JWVHCZEvb1lSoriginalMkMtnMgllP";
	op_parser.RecordInsert(rec_insert);
	ASSERT_EQ(op_parser.table_data_.datas_.size(), 1);
	ASSERT_EQ(op_parser.table_data_.pk2row.size(), 1);
	ASSERT_EQ(op_parser.table_data_.pk2row["66"], op_parser.table_data_.datas_.begin());
	// Insert
	const char* rec_insert2 = "I	tianchi_dts_data	item	12	6118	4xl4nzzzzzzzOg3G	5.36	Y52pyRjzzzzzzzzaaanalMkMtnMgllP";
	op_parser.RecordInsert(rec_insert2);
	ASSERT_EQ(op_parser.table_data_.datas_.size(), 2);
	ASSERT_EQ(op_parser.table_data_.pk2row.size(), 2);
	ASSERT_EQ(op_parser.table_data_.pk2row["12"], op_parser.table_data_.datas_.begin());
	ASSERT_EQ(op_parser.table_data_.pk2row["66"], ++op_parser.table_data_.datas_.begin());

	db_transfer::IOHandler io_handler;
	io_handler.SaveTableData(op_parser.table_data_, "./out/test-table");
}