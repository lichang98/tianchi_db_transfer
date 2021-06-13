#include "include/table_handler.h"

void db_transfer::TableData::SortRowsByPK(Table table_meta) {
    std::vector<int> pk_col_idxs;
    for (auto index : table_meta.indexs_) {
        if (index.primary_) {
            for (auto col_name : index.index_cols_) {
                int col_idx = table_meta.name2col_[col_name];
                pk_col_idxs.emplace_back(col_idx);
            }
        }
    }

    this->datas_.sort([&pk_col_idxs, &table_meta](TableRow &a, TableRow &b)->bool{
        for (auto col_idx : pk_col_idxs) {
            if (table_meta.columns_[col_idx].col_def_.find("int") != std::string::npos) {
                int key1 = std::stoi(a[col_idx]);
                int key2 = std::stoi(b[col_idx]);
                if (key1 != key2) {
                    return key1 < key2;
                }
            } else if (table_meta.columns_[col_idx].col_def_.find("char") != std::string::npos) {
                if (a[col_idx] != b[col_idx]) {
                    return a[col_idx] < b[col_idx];
                }
            } else {
                double key1 = std::atof(a[col_idx].c_str());
                double key2 = std::atof(b[col_idx].c_str());
                if (key1 != key2) {
                    return key1 < key2;
                }
            }
        }
        return false;
    });
}