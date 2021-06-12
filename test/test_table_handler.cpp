#include "include/parser.h"

#include <gtest/gtest.h>

TEST(TEST_TABLE_HANDLER, SORT_ROWS) {
    const char* table_meta_info = "[DATABASE] tianchi_dts_data [TABLE] new_orders\n\
COLUMN NUMBER 3\n\
{\"Name\":\"no_o_id\",\"Ordinal\":1,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"int(11)\",\"Length\":null,\"Precision\":10,\"Scale\":0}\n\
{\"Name\":\"no_d_id\",\"Ordinal\":2,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"tinyint(4)\",\"Length\":null,\"Precision\":3,\"Scale\":0}\n\
{\"Name\":\"no_w_id\",\"Ordinal\":3,\"Unsigned\":false,\"CharSet\":null,\"ColumnDef\":\"smallint(6)\",\"Length\":null,\"Precision\":5,\"Scale\":0}\n\
INDEX NUMBER 3\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"no_w_id\"],\"Primary\":true,\"Unique\":true}\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"no_d_id\"],\"Primary\":true,\"Unique\":true}\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"no_o_id\"],\"Primary\":true,\"Unique\":true}\n\
PRIMARY KEY NUMBER 3\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"no_w_id\"],\"Primary\":true,\"Unique\":true}\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"no_d_id\"],\"Primary\":true,\"Unique\":true}\n\
{\"IndexName\":\"PRIMARY\",\"IndexCols\":[\"no_o_id\"],\"Primary\":true,\"Unique\":true}";

    db_transfer::Table table_meta = db_transfer::ParseTable(table_meta_info);
    db_transfer::OperationParser op_parser(table_meta);
    
    ASSERT_EQ(op_parser.primary_key_idxs.size(), 3);
    
    op_parser.ProcessOp("I	tianchi_dts_data	new_orders	2117	1	1");
    op_parser.ProcessOp("I	tianchi_dts_data	new_orders	2117	1	3");
    op_parser.ProcessOp("I	tianchi_dts_data	new_orders	2117	1	2");
    op_parser.ProcessOp("I	tianchi_dts_data	new_orders	2112	1	4");
    op_parser.ProcessOp("I	tianchi_dts_data	new_orders	2112	2	3");
    ASSERT_EQ(op_parser.table_data_.datas_.size(), 5);
    op_parser.table_data_.SortRowsByPK(op_parser.table_meta_);
    ASSERT_EQ(op_parser.table_data_.datas_.begin()->fields_[0], "2112");
    ASSERT_EQ(op_parser.table_data_.datas_.begin()->fields_[1], "1");
    ASSERT_EQ(op_parser.table_data_.datas_.begin()->fields_[2], "4");
}