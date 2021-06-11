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
	
	ASSERT_TRUE(db_table.columns_.find("i_id") != db_table.columns_.end());
	ASSERT_TRUE(db_table.columns_.find("i_im_id") != db_table.columns_.end());
	ASSERT_TRUE(db_table.columns_.find("i_name") != db_table.columns_.end());
	ASSERT_TRUE(db_table.columns_.find("i_price") != db_table.columns_.end());
	ASSERT_TRUE(db_table.columns_.find("i_data") != db_table.columns_.end());

	ASSERT_EQ(db_table.columns_["i_id"].ordinal_, 1);
	ASSERT_EQ(db_table.columns_["i_id"].unsign_, false);
	ASSERT_EQ(db_table.columns_["i_id"].charset_, "");
	ASSERT_EQ(db_table.columns_["i_id"].col_def_, "int(11)");
	ASSERT_EQ(db_table.columns_["i_id"].len_, -1);
	ASSERT_EQ(db_table.columns_["i_id"].precis_, 10);
	ASSERT_EQ(db_table.columns_["i_id"].scale_, 0);

	ASSERT_EQ(db_table.indexs_.size(), 1);
	ASSERT_EQ(db_table.indexs_[0].index_name_, "PRIMARY");
	ASSERT_EQ(db_table.indexs_[0].index_cols_.size(), 1);
	ASSERT_EQ(db_table.indexs_[0].primary_, true);
	ASSERT_EQ(db_table.indexs_[0].unique_, true);
}