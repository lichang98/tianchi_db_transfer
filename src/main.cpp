#include "include/parser.h"

#include <filesystem>
#include <iostream>
#include <tuple>

std::vector<std::filesystem::path> GetSourceFiles(const char* input_dir) {
    std::vector<std::filesystem::path> input_file_paths;
    for (auto file_entry : std::filesystem::directory_iterator(input_dir)) {
        input_file_paths.emplace_back(file_entry.path());
    }
    std::sort(input_file_paths.begin(), input_file_paths.end(), [](const std::filesystem::path &a, const std::filesystem::path &b)->bool {
        int ord_a = a.filename().generic_string().find_last_of('_') + 1;
        int ord_b = b.filename().generic_string().find_last_of('_') + 1;
        ord_a = std::stoi(a.filename().generic_string().substr(ord_a));
        ord_b = std::stoi(b.filename().generic_string().substr(ord_b));
        return ord_a < ord_b;
    });
    return input_file_paths;
}

std::tuple<std::vector<db_transfer::Table>, std::unordered_map<std::string, int>> LoadTableMeta(std::string schema_file) {
    std::vector<db_transfer::Table> table_metas;
    std::unordered_map<std::string, int> table_name2meta_idx;
    db_transfer::IOHandler io_handler;
    bool avail = true;
    int idx = 0;
    std::string table_mt_str = io_handler.LoadSchema(schema_file, avail);
    while (avail) {
        db_transfer::Table table_meta = db_transfer::ParseTable(table_mt_str);
        table_metas.emplace_back(table_meta);
        std::cout << "Loading schema table_name=" << table_meta.table_name_ << std::endl;
        table_name2meta_idx.insert(std::make_pair(table_meta.table_name_, idx++));
        table_mt_str = io_handler.LoadSchema(schema_file, avail);
    }
    return std::make_tuple(table_metas, table_name2meta_idx);
}

/**
 * argvs:
 * 0: current program
 * 1: input dir
 * 2: output_dir
 * 3: output_db_url
 * 4: output_db_user
 * 5: output_db_passwd
 */
int main(int argc, char const *argv[]) {
    std::vector<std::filesystem::path> input_files = GetSourceFiles("./demo-test/source_file_dir/source_file_dir");
    std::string schema_file = "./demo-test/schema_info_dir/schema.info";
    std::tuple<std::vector<db_transfer::Table>, std::unordered_map<std::string, int>> ret = \
                LoadTableMeta(schema_file);
    std::vector<db_transfer::Table> table_metas = std::get<0>(ret);
    std::unordered_map<std::string, int> table_name2meta_idx = std::get<1>(ret);
    
    std::vector<db_transfer::OperationParser> op_parsers;
    for (auto &meta : table_metas) {
        db_transfer::OperationParser op_pser(meta);
        op_parsers.emplace_back(std::move(op_pser));
    }

    db_transfer::IOHandler io_handler;

    for (auto ele : table_name2meta_idx) {
        std::cout << ele.first << " ===> " << ele.second << std::endl;
    }

    for (auto in_file : input_files) {
        bool avail = true;
        std::string table_name;
        std::string op_line = io_handler.LoadSrcDataNextLine(in_file.generic_string(), avail, table_name);
        std::cout << "Processing input file: " << in_file.filename() << " ..." << std::endl;
        while (avail) {
            int idx = table_name2meta_idx[table_name];
            op_parsers[idx].ProcessOp(op_line);
            op_line = io_handler.LoadSrcDataNextLine(in_file.generic_string(), avail, table_name);
        }
    }

    // Save records for each table
    for (auto out_data : op_parsers) {
        std::cout << "Saving data of table " << out_data.table_meta_.table_name_ << ", data size=" << out_data.table_data_.datas_.size() << std::endl;
        out_data.table_data_.SortRowsByPK(out_data.table_meta_);
        io_handler.SaveTableData(out_data.table_data_, "./out/"+out_data.table_meta_.table_name_);
    }

    return 0;
}

