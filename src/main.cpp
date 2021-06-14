#include "include/parser.h"

#include <iostream>
#include <tuple>

#include <dirent.h>

const std::string DATABASE_NAME = "tianchi_dts_data";
const std::string SCHEMA_FILE_DIR = "schema_info_dir";
const std::string SCHEMA_FILE_NAME = "schema.info";
const std::string SOURCE_FILE_DIR = "source_file_dir";
const std::string SINK_FILE_DIR = "sink_file_dir";
const std::string SOURCE_FILE_NAME_TEMPLATE = "tianchi_dts_source_data_";
const std::string SINK_FILE_NAME_TEMPLATE = "tianchi_dts_sink_data_";

std::vector<std::string> GetSourceFiles(std::string input_dir) {
    std::vector<std::string> input_file_paths;
    DIR *dir;
    dirent *ent;
    if ((dir = opendir(input_dir.c_str())) != nullptr) {
        while ((ent = readdir(dir)) != nullptr) {
            if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) {
                continue;
            }
            input_file_paths.emplace_back(input_dir+"/"+std::string(ent->d_name));
        }
        closedir(dir);
    }

    std::sort(input_file_paths.begin(), input_file_paths.end(), [](const std::string &a, const std::string &b) -> bool {
        int ord_a = a.find_last_of('_') + 1;
        int ord_b = b.find_last_of('_') + 1;
        ord_a = std::stoi(a.substr(ord_a));
        ord_b = std::stoi(b.substr(ord_b));
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
    std::vector<std::string> input_files = GetSourceFiles(std::string(argv[1])+"/"+SOURCE_FILE_DIR);
    std::string schema_file = std::string(argv[1])+"/"+SCHEMA_FILE_DIR+"/"+SCHEMA_FILE_NAME;
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
        std::string op_line = io_handler.LoadSrcDataNextLine(in_file, avail, table_name);
        std::cout << "Processing input file: " << in_file << " ..." << std::endl;
        while (avail) {
            int idx = table_name2meta_idx[table_name];
            op_parsers[idx].ProcessOp(op_line);
            op_line = io_handler.LoadSrcDataNextLine(in_file, avail, table_name);
        }
    }

    // Save records for each table
    // std::filesystem::create_directory(std::string(argv[2])+"/"+SINK_FILE_DIR);
    for (auto out_data : op_parsers) {
        std::cout << "Saving data of table " << out_data.table_meta_.table_name_ << ", data size=" << out_data.table_data_.datas_.size() << std::endl;
        out_data.table_data_.SortRowsByPK(out_data.table_meta_);
        io_handler.SaveTableData(out_data.table_data_, std::string(argv[2])+"/"+SINK_FILE_DIR+"/"+SINK_FILE_NAME_TEMPLATE+out_data.table_meta_.table_name_);
    }

    return 0;
}

