#include "include/parser.h"

#include <iostream>
#include <tuple>
#include <thread>
#include <queue>
#include <mutex>
#include <chrono>

#include <dirent.h>

const std::string DATABASE_NAME = "tianchi_dts_data";
const std::string SCHEMA_FILE_DIR = "schema_info_dir";
const std::string SCHEMA_FILE_NAME = "schema.info";
const std::string SOURCE_FILE_DIR = "source_file_dir";
const std::string SINK_FILE_DIR = "sink_file_dir";
const std::string SOURCE_FILE_NAME_TEMPLATE = "tianchi_dts_source_data_";
const std::string SINK_FILE_NAME_TEMPLATE = "tianchi_dts_sink_data_";
const int QUEUE_MAX_SIZE = 100;
// Wait for n milliseconds when queue is empty
const int TABLE_WAIT_QUEUE_EMPTY_TIMEOUT = 1000;

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
    std::iostream::sync_with_stdio(false);
    
    std::vector<std::string> input_files = GetSourceFiles(std::string(argv[1])+"/"+SOURCE_FILE_DIR);
    std::string schema_file = std::string(argv[1])+"/"+SCHEMA_FILE_DIR+"/"+SCHEMA_FILE_NAME;
    const std::string output_dir = std::string(argv[2]) + "/" + SINK_FILE_DIR;
    std::tuple<std::vector<db_transfer::Table>, std::unordered_map<std::string, int>> ret = \
                LoadTableMeta(schema_file);
    std::vector<db_transfer::Table> table_metas = std::get<0>(ret);
    std::unordered_map<std::string, int> table_name2meta_idx = std::get<1>(ret);
    
    std::vector<db_transfer::OperationParser> op_parsers;
    for (auto &meta : table_metas) {
        db_transfer::OperationParser op_pser(meta);
        op_parsers.emplace_back(std::move(op_pser));
    }

    // db_transfer::IOHandler io_handler;

    for (auto ele : table_name2meta_idx) {
        std::cout << ele.first << " ===> " << ele.second << std::endl;
    }

    std::vector<std::queue<std::pair<std::string, db_transfer::FilePos>>> tables_queues(table_name2meta_idx.size());
    std::vector<std::mutex> queues_mutexs(table_name2meta_idx.size());
    std::vector<std::unordered_map<std::string, db_transfer::FilePos>> table_valid_records(table_name2meta_idx.size());

    // Each source file one thread
    // TODO
    auto thread_routine = [&tables_queues, &queues_mutexs, &table_name2meta_idx, &op_parsers](std::string src_file, int curr_idx)->void {
        // Read lines from src_file, and put records onto corresponding table's queue, need mutex lock
        db_transfer::IOHandler io_handler;
        bool data_avail = true;
        std::string table_name;
        int64_t curr_pos = 0;
        std::string line = io_handler.LoadSrcDataNextLine(src_file, data_avail, table_name);
        int table_idx = table_name2meta_idx[table_name];
        std::string pk_str = op_parsers[table_idx].GetPKFromSrcRowRecord(line);
        while (data_avail) {
            std::pair<std::string, db_transfer::FilePos> row_record = std::make_pair(pk_str, db_transfer::FilePos(curr_idx, curr_pos));
            queues_mutexs[table_idx].lock();
            tables_queues[table_idx].push(std::move(row_record));
            queues_mutexs[table_idx].unlock();

            curr_pos = io_handler.GetCurrPosition();
            line = io_handler.LoadSrcDataNextLine(src_file, data_avail, table_name);
            if (data_avail) {
                table_idx = table_name2meta_idx[table_name];
                pk_str = op_parsers[table_idx].GetPKFromSrcRowRecord(line);
            }
        }
    };

    auto table_thread_routine = [&tables_queues, &queues_mutexs, &op_parsers, &table_valid_records, &input_files, &output_dir](int table_idx)->void {
        std::pair<std::string, db_transfer::FilePos> record;
        while (true) {
            if (tables_queues[table_idx].empty()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(TABLE_WAIT_QUEUE_EMPTY_TIMEOUT));
                if (tables_queues[table_idx].empty()) {
                    break;
                }
            }
            // Fetch one record from queue, check conflict of primary key and update records
            queues_mutexs[table_idx].lock();
            record = tables_queues[table_idx].front();
            tables_queues[table_idx].pop();
            queues_mutexs[table_idx].unlock();
            // std::cout << "get record = " << record << std::endl;
            if (table_valid_records[table_idx].find(record.first) == table_valid_records[table_idx].end()) {
                table_valid_records[table_idx].insert(record);
            } else {
                int new_file_no = record.second.file_no_;
                int new_file_pos = record.second.pos_;
                int old_file_no = table_valid_records[table_idx][record.first].file_no_;
                int old_file_pos = table_valid_records[table_idx][record.first].pos_;
                if (new_file_no > old_file_no || (new_file_no == old_file_no && new_file_pos > old_file_pos)) {
                    table_valid_records[table_idx][record.first] = record.second;
                }
            }
        }

        // Saving into output file of the table when source files finished
        // Read from source files according to 'table_valid_records', verify and then write to output file
        std::vector<std::pair<std::string, db_transfer::FilePos>> valid_records;
        for (auto &ele : table_valid_records[table_idx]) {
            valid_records.emplace_back(std::move(ele));
        }
        auto &cols_meta = op_parsers[table_idx].table_meta_;
        auto &table_parser = op_parsers[table_idx];
        // Sort by primary key
        std::sort(valid_records.begin(), valid_records.end(), [&cols_meta, &table_parser](const std::pair<std::string, db_transfer::FilePos> &a, \
                                const std::pair<std::string, db_transfer::FilePos> &b)->bool{
            std::stringstream ss_a(a.first);
            std::stringstream ss_b(b.first);
            std::string field_a, field_b;
            int idx = 0;
            while (std::getline(ss_a, field_a, '|') && std::getline(ss_b, field_b, '|')) {
                if (cols_meta.columns_[table_parser.primary_key_idxs[idx]].col_def_.find("int") != std::string::npos) {
                    int key1 = std::stoi(field_a);
                    int key2 = std::stoi(field_b);
                    if (key1 != key2) { return key1 < key2; }
                    // return std::stoi(field_a) < std::stoi(field_b);
                } else if (cols_meta.columns_[table_parser.primary_key_idxs[idx]].col_def_.find("char") != std::string::npos) {
                    if (field_a != field_b) { return field_a < field_b; }
                } else {
                    double key1 = std::stof(field_a);
                    double key2 = std::stof(field_b);
                    if (key1 != key2) { return key1 < key2; }
                }
                idx++;
            }
            return false;
        });


        std::vector<std::fstream> file_handlers;
        for (auto &file_path : input_files) {
            std::fstream fs;
            fs.open(file_path, std::fstream::in);
            file_handlers.emplace_back(std::move(fs));
        }
        std::fstream output_fhandler;
        output_fhandler.open(std::move(std::string(output_dir+"/"+op_parsers[table_idx].table_meta_.table_name_)), std::fstream::out);
        char *buf = new char[2048];

        auto output_routine = [&table_parser, buf, &file_handlers, &output_fhandler](std::pair<std::string, db_transfer::FilePos> &rec_pos)->void {
            auto &f_handler = file_handlers[rec_pos.second.file_no_ - 1];
            f_handler.seekg(rec_pos.second.pos_, f_handler.beg);
            f_handler.getline(buf, 2048);
            std::vector<std::string> vec_row_recs = table_parser.RowRecordsAsVec(std::string(buf));
            // Verify fields
            for (int i = 0; i < vec_row_recs.size(); ++i) {
                vec_row_recs[i] = db_transfer::VerifyValid(vec_row_recs[i], table_parser.table_meta_.columns_[i].col_def_);
            }
            // Write to output file
            int i = 0;
            for (; i < vec_row_recs.size() - 1; ++i) {
                output_fhandler << vec_row_recs[i] << "\t";
            }
            output_fhandler << vec_row_recs[i];
        };

        output_routine(valid_records[0]);
        for (int i = 1; i < valid_records.size(); ++i) {
            output_fhandler << "\n";
            output_routine(valid_records[i]);
        }
        delete[] buf;
        for (auto &fs : file_handlers) {
            fs.close();
        }
        output_fhandler.close();
    };

    // Start source files processing threads
    for (auto &in_file : input_files) {
        int idx = in_file.find_last_of('_') + 1;
        idx = std::stoi(in_file.substr(idx));
        // std::thread th(thread_routine, in_file, idx);
        std::cout << "in _file = " << in_file << ", idx=" << idx << std::endl;
        thread_routine(in_file, idx);
    }

    std::cout << "started all thread routine.." << std::endl;
    std::vector<std::thread> table_threads;
    // Start table threads
    for (int i = 0; i < table_metas.size(); ++i) {
        std::thread th(table_thread_routine, i);
        table_threads.emplace_back(std::move(th));
    }

    for (int i = 0; i < table_metas.size(); ++i) {
        table_threads[i].join();
    }
    std::cout << "Done." << std::endl;
    return 0;
}

