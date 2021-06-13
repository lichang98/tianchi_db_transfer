#include "include/parser.h"

#include <gtest/gtest.h>

TEST(PARSER, TEST_COL_PARSE) {
	const char* col_meta = "{\"Name\":\"i_id\",\"Ordinal\":1,\"Unsigned\":false,\
				\"CharSet\":null,\"ColumnDef\":\"int(11)\",\"Length\":null,\
				\"Precision\":10,\"Scale\":0}";
	db_transfer::Column column_res = db_transfer::ParseColumnMeta(col_meta);
	ASSERT_EQ(column_res.col_name_, "i_id");
	ASSERT_EQ(column_res.ordinal_, 1);
	ASSERT_EQ(column_res.unsign_, false);
	ASSERT_EQ(column_res.charset_, "");
	ASSERT_EQ(column_res.col_def_, "int(11)");
	ASSERT_EQ(column_res.len_, -1);
	ASSERT_EQ(column_res.precis_, 10);
	ASSERT_EQ(column_res.scale_, 0);
}

TEST(PARSER, TEST_INDEX_PARSE) {
	const char* index_meta = "{\"IndexName\":\"PRIMARY\",\"IndexCols\":\
								[\"d_w_id\"],\"Primary\":true,\"Unique\":true}";
	db_transfer::Index index_res = db_transfer::ParseIndexMeta(index_meta);
	ASSERT_EQ(index_res.index_name_, "PRIMARY");
	ASSERT_EQ(index_res.primary_, true);
	ASSERT_EQ(index_res.unique_, true);
	ASSERT_EQ(index_res.index_cols_.size(), 1);
	ASSERT_EQ(index_res.index_cols_[0], "d_w_id");
}

TEST(PARSER, TEST_TABLE_PARSE) {
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
	ASSERT_EQ(db_table.db_name_, "tianchi_dts_data");
	ASSERT_EQ(db_table.table_name_, "item");

	ASSERT_TRUE(db_table.name2col_.find("i_id") != db_table.name2col_.end());
	ASSERT_TRUE(db_table.name2col_.find("i_im_id") != db_table.name2col_.end());
	ASSERT_TRUE(db_table.name2col_.find("i_name") != db_table.name2col_.end());
	ASSERT_TRUE(db_table.name2col_.find("i_price") != db_table.name2col_.end());
	ASSERT_TRUE(db_table.name2col_.find("i_data") != db_table.name2col_.end());

	ASSERT_EQ(db_table.columns_[db_table.name2col_["i_id"]].ordinal_, 1);
	ASSERT_EQ(db_table.columns_[db_table.name2col_["i_id"]].unsign_, false);
	ASSERT_EQ(db_table.columns_[db_table.name2col_["i_id"]].charset_, "");
	ASSERT_EQ(db_table.columns_[db_table.name2col_["i_id"]].col_def_, "int(11)");
	ASSERT_EQ(db_table.columns_[db_table.name2col_["i_id"]].len_, -1);
	ASSERT_EQ(db_table.columns_[db_table.name2col_["i_id"]].precis_, 10);
	ASSERT_EQ(db_table.columns_[db_table.name2col_["i_id"]].scale_, 0);

	ASSERT_EQ(db_table.indexs_.size(), 1);
	ASSERT_EQ(db_table.indexs_[0].index_name_, "PRIMARY");
	ASSERT_EQ(db_table.indexs_[0].index_cols_.size(), 1);
	ASSERT_EQ(db_table.indexs_[0].primary_, true);
	ASSERT_EQ(db_table.indexs_[0].unique_, true);
}

TEST(PARSER, TEST_ROW_RECORDS2VEC) {
	const char* table_meta = "[DATABASE] tianchi_dts_data [TABLE] orders\n\
COLUMN NUMBER 8\n\
{\"Name\":\"o_id\",\"Ordinal\":1,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"int(11)\",\"Length\":null,\"Precision\":10,\"Scale\":0}\n\
{\"Name\":\"o_d_id\",\"Ordinal\":2,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"tinyint(4)\",\"Length\":null,\"Precision\":3,\"Scale\":0}\n\
{\"Name\":\"o_w_id\",\"Ordinal\":3,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"smallint(6)\",\"Length\":null,\"Precision\":5,\"Scale\":0}\n\
{\"Name\":\"o_c_id\",\"Ordinal\":4,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"int(11)\",\"Length\":null,\"Precision\":10,\"Scale\":0}\n\
{\"Name\":\"o_entry_d\",\"Ordinal\":5,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"datetime\",\"Length\":null,\"Precision\":null,\"Scale\":null}\n\
{\"Name\":\"o_carrier_id\",\"Ordinal\":6,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"tinyint(4)\",\"Length\":null,\"Precision\":3,\"Scale\":0}\n\
{\"Name\":\"o_ol_cnt\",\"Ordinal\":7,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"tinyint(4)\",\"Length\":null,\"Precision\":3,\"Scale\":0}\n\
{\"Name\":\"o_all_local\",\"Ordinal\":8,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"tinyint(4)\",\"Length\":null,\"Precision\":3,\"Scale\":0}\n\
INDEX NUMBER 7\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"o_w_id\"],\"Primary\":true,\"Unique\":true}\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"o_d_id\"],\"Primary\":true,\"Unique\":true}\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"o_id\"],\"Primary\":true,\"Unique\":true}\n\
{\"IndexName\":\"idx_orders\",\"IndexCols\":[\"o_w_id\"],\"Primary\":false,\"Unique\":false}\n\
{\"IndexName\":\"idx_orders\",\"IndexCols\":[\"o_d_id\"],\"Primary\":false,\"Unique\":false}\n\
{\"IndexName\":\"idx_orders\",\"IndexCols\":[\"o_c_id\"],\"Primary\":false,\"Unique\":false}\n\
{\"IndexName\":\"idx_orders\",\"IndexCols\":[\"o_id\"],\"Primary\":false,\"Unique\":false}\n\
PRIMARY KEY NUMBER 3\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"o_w_id\"],\"Primary\":true,\"Unique\":true}\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"o_d_id\"],\"Primary\":true,\"Unique\":true}\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"o_id\"],\"Primary\":true,\"Unique\":true}";
	db_transfer::Table db_table = db_transfer::ParseTable(table_meta);
	db_transfer::OperationParser op_parser(db_table);
	const char* row_rec = "I\ttianchi_dts_data\torders\t5\t1\t1\t1517\t2021-04-11 15:41:55.0\t9\t6\t1";
	
	std::vector<std::string> vec_recs = op_parser.RowRecordsAsVec(row_rec);	

	ASSERT_EQ(vec_recs.size(), 8);
	ASSERT_EQ(vec_recs[0], "5");
	ASSERT_EQ(vec_recs[1], "1");
	ASSERT_EQ(vec_recs[2], "1");
	ASSERT_EQ(vec_recs[3], "1517");
	ASSERT_EQ(vec_recs[4], "2021-04-11 15:41:55.0");
	ASSERT_EQ(vec_recs[5], "9");
	ASSERT_EQ(vec_recs[6], "6");
	ASSERT_EQ(vec_recs[7], "1");
}

TEST(PARSER, TEST_IABD_OP) {
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
	ASSERT_EQ(op_parser.table_data_.pk2row["66|"], op_parser.table_data_.datas_.begin());
	// Insert
	const char* rec_insert2 = "I	tianchi_dts_data	item	12	6118	4xl4nzzzzzzzOg3G	5.36	Y52pyRjzzzzzzzzaaanalMkMtnMgllP";
	op_parser.RecordInsert(rec_insert2);
	ASSERT_EQ(op_parser.table_data_.datas_.size(), 2);
	ASSERT_EQ(op_parser.table_data_.pk2row.size(), 2);
	ASSERT_EQ(op_parser.table_data_.pk2row["12|"], op_parser.table_data_.datas_.begin());
	ASSERT_EQ(op_parser.table_data_.pk2row["66|"], ++op_parser.table_data_.datas_.begin());
	// Update before
	const char* rec_before_update1 = "B	tianchi_dts_data	item	66	6858	4xl4nqtG6Ij1Og3G	8.46	Y52pyRj6JWVHCZEvb1lSoriginalMkMtnMgllP";
	op_parser.RecordPendingUpdate(rec_before_update1);
	ASSERT_EQ(op_parser.table_data_.update_before_queue_.size(), 1);
	// Delete
	op_parser.RecordDelete(rec_insert2);
	ASSERT_EQ(op_parser.table_data_.datas_.size(), 1);
	ASSERT_EQ(op_parser.table_data_.pk2row.size(), 1);
	ASSERT_EQ(op_parser.table_data_.update_before_queue_.size(), 1);
	ASSERT_TRUE(op_parser.table_data_.pk2row.find("12|") == op_parser.table_data_.pk2row.end());
	// Update after
	const char* rec_after_update1 = "A	tianchi_dts_data	item	67	1158	4xl111116Ij1Og3G	8.46	Y52py1111111vb1lSoriginalMkMtnMgllP";
	op_parser.RecordUpdate(rec_after_update1);
	ASSERT_EQ(op_parser.table_data_.datas_.size(), 1);
	ASSERT_TRUE(op_parser.table_data_.pk2row.find("66|") == op_parser.table_data_.pk2row.end());
	ASSERT_TRUE(op_parser.table_data_.pk2row.find("67|") != op_parser.table_data_.pk2row.end());
	ASSERT_EQ(op_parser.table_data_.pk2row["67|"], op_parser.table_data_.datas_.begin());
	// Insert primary key already in the table
	const char* rep_pk_row = "A	tianchi_dts_data	item	67	1158	4xxxxxxxxxxxxG	8.46	Y52pyaaaaaaaaaaaaaaallP";
	op_parser.RecordInsert(rep_pk_row);
	ASSERT_EQ(op_parser.table_data_.datas_.size(), 1);
	ASSERT_EQ(op_parser.table_data_.pk2row.size(), 1);
	ASSERT_EQ(op_parser.table_data_.update_before_queue_.size(), 0);
	ASSERT_TRUE(op_parser.table_data_.pk2row.find("67|") != op_parser.table_data_.pk2row.end());
	ASSERT_EQ(op_parser.table_data_.pk2row["67|"]->fields_[2], "4xxxxxxxxxxxxG");
}

TEST(PARSER, VERIFY_FIELD) {
	std::string val = db_transfer::VerifyValid("2021-04-11 15:41:55o0", "datetime");
	ASSERT_EQ(val, "2020-04-01 00:00:00.0");
	val = db_transfer::VerifyValid("2021-04-11 15:41:55.0", "datetime");
	ASSERT_EQ(val, "2021-04-11 15:41:55.0");
	val = db_transfer::VerifyValid("1jjiqiioskkkkkkkaq", "char(16)");
	ASSERT_EQ(val, "1jjiqiioskkkkkkk");
	val = db_transfer::VerifyValid("1jjiqiioskkkkkkkaq", "varchar(16)");
	ASSERT_EQ(val, "1jjiqiioskkkkkkk");
	val = db_transfer::VerifyValid("19100oo", "int(11)");
	ASSERT_EQ(val, "0");
	val = db_transfer::VerifyValid("10192", "smallint(5)");
	ASSERT_EQ(val, "10192");
	val = db_transfer::VerifyValid("199.995", "decimal(10, 2)");
	ASSERT_EQ(val, "200.00");
	val = db_transfer::VerifyValid("199.99488", "decimal(10, 2)");
	ASSERT_EQ(val, "199.99");
	val = db_transfer::VerifyValid("199.99", "decimal(10, 2)");
	ASSERT_EQ(val, "199.99");
	val = db_transfer::VerifyValid("0.00180", "decimal(10, 2)");
	ASSERT_EQ(val, "0.00");
	val = db_transfer::VerifyValid("0.00721", "decimal(10,2)");
	ASSERT_EQ(val, "0.01");
}