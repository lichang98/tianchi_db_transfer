#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <string>
#include <sstream>
#include <fstream>
#include <iostream>
#include <algorithm>

#include "rapidjson/document.h"
#include "rapidjson/writer.h"
#include "rapidjson/stringbuffer.h"

#include <vector>
#include <unordered_map>
#include <map>
#include <thread>
#include <queue>
#include <mutex>
#include <unordered_set>
#include <chrono>
#include <iomanip>


struct column_t {
    std::string col_name_;
    int32_t ord_;
    bool unsign_;
    std::string charset_;
    std::string col_def_;
    // Only for varchar like type, if this field is null, -1
    int32_t len_;
    // Only for digit type, otherwise -1
    int32_t precis_;
    // Only for decimal, otherwise -1
    int32_t scale_;
};

struct index_t {
    std::string name_;
    std::vector<int> index_cols_;
    bool primary_;
    bool unique_;
};

struct table_t {
    std::string db_name_;
    std::string table_name_;
    std::vector<column_t> colunms_;
    std::vector<index_t> indexs_;

    std::unordered_map<std::string, int> name2col_;
};

struct record_meta_t {
    int64_t pos_;
    int32_t file_no_;
};

struct column_t ParseColumnMeta(std::string &table_meta_buf) {
    struct column_t col;
    rapidjson::Document doc;
    doc.Parse(table_meta_buf.c_str());
    
    col.col_name_ = doc["Name"].GetString();
    col.ord_ = doc["Ordinal"].GetInt();
    col.unsign_ = doc["Unsigned"].GetBool();
    if (!doc["CharSet"].IsNull()) {
        col.charset_ = doc["CharSet"].GetString();
    }
    col.col_def_ = doc["ColumnDef"].GetString();
    if (doc["Length"].IsNull()) {
        col.len_ = -1;
    } else {
        col.len_ = doc["Length"].GetInt();
    }

    if (doc["Precision"].IsNull()) {
        col.precis_ = -1;
    } else {
        col.precis_ = doc["Precision"].GetInt();
    }

    if (doc["Scale"].IsNull()) {
        col.scale_ = -1;
    } else {
        col.scale_ = doc["Scale"].GetInt();
    }
    return col;
}

struct index_t ParseIndexMeta(std::string &index_meta_buf, table_t &table_meta) {
    struct index_t index;
    rapidjson::Document doc;
    doc.Parse(index_meta_buf.c_str());
    index.name_ = doc["IndexName"].GetString();
    index.primary_ = doc["Primary"].GetBool();
    index.unique_ = doc["Unique"].GetBool();
    int index_col_size = doc["IndexCols"].GetArray().Size();
    auto col_arr = doc["IndexCols"].GetArray();
    for (int i = 0; i < index_col_size; ++i) {
        index.index_cols_.emplace_back(table_meta.name2col_.at(col_arr[i].GetString()));
    }
    return std::move(index);
}

struct table_t ParseTable(const std::string &multi_line) {
    struct table_t table;
    std::stringstream ss;
    ss << multi_line;
    std::string str;
    int val;
    ss >> str;
    ss >> str;
    table.db_name_ = str;
    ss >> str;
    ss >> str;
    table.table_name_ = str;
    ss >> str;
    ss >> str;
    // Columns
    ss >> val;
    table.colunms_.reserve(val);
    for (int i = 0; i < val; ++i) {
        ss >> str;
        column_t&& col = ParseColumnMeta(str);
        table.colunms_.emplace_back(col);
        table.name2col_.insert(std::make_pair(col.col_name_, i));
    }
    ss >> str;
    ss >> str;
    // Indexs
    ss >> val;
    table.indexs_.reserve(val);
    for (int i = 0; i < val; ++i) {
        ss >> str;
        struct index_t&& index = ParseIndexMeta(str, table);
        table.indexs_.emplace_back(index);
    }
    return std::move(table);
}

std::vector<struct table_t> LoadTableMeta(const std::string &meta_file) {
    std::vector<struct table_t> tables;
    std::fstream fs;
    std::stringstream ss;
    const int line_size = 1024;
    char *line = new char[line_size];

    fs.open(meta_file, std::fstream::in);
    bzero(line, line_size);
    fs.getline(line, line_size);
    ss << line << "\n";
    while (!fs.eof()) {
        bzero(line, line_size);
        fs.getline(line, line_size);
        if (strstr(line, "[DATABASE]") != nullptr) {
            struct table_t&& tbl = ParseTable(ss.str());
            tables.emplace_back(tbl);
            ss.str("");
        }
        ss << line << "\n";
    }
    struct table_t&& tbl = ParseTable(ss.str());
    tables.emplace_back(tbl);

    fs.close();
    delete[] line;
    return std::move(tables);
}

void show_table_meta(struct table_t &tbl) {
    std::cout << "table name=" << tbl.table_name_ << ", cols size=" << tbl.colunms_.size() \
                << ", index size=" << tbl.indexs_.size() << std::endl;
    for (auto &col : tbl.colunms_) {
        std::cout << "\t" << "col name=" << col.col_name_ << ", col_def=" << col.col_def_ \
                << ", col ord=" << col.ord_ << std::endl;
    }
}

std::vector<std::string> GetSrcFiles(const std::string &input_dir) {
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

    std::sort(input_file_paths.begin(), input_file_paths.end(), \
                    [](const std::string &a, const std::string &b) -> bool {
        int ord_a = a.find_last_of('_') + 1;
        int ord_b = b.find_last_of('_') + 1;
        ord_a = std::stoi(a.substr(ord_a));
        ord_b = std::stoi(b.substr(ord_b));
        return ord_a < ord_b;
    });
    return input_file_paths;
}

bool CheckDateTime(std::string &val) {
    if (val.size() > 26) { return false; }
    if (!isdigit(val[0]) || !isdigit(val[1]) || !isdigit(val[2]) || !isdigit(val[3]) \
        ||!isdigit(val[5]) || !isdigit(val[6]) || !isdigit(val[8]) || !isdigit(val[9])\
        || !isdigit(val[11]) || !isdigit(val[12]) || !isdigit(val[14]) || !isdigit(val[15])\
        || !isdigit(val[17]) || !isdigit(val[18])) {
            return false;
    }
    if (val[4] != '-' || val[7] != '-') { return false; }
    if (val[10] != ' ' || val[13] != ':' || val[16] != ':') { return false; }
    if (val.size() > 19) {
        if (val[19] != '.') { return false; }
        for (int i = val.size() - 1; i >= 20; --i) {
            if (!isdigit(val[i])) { return false; }
        }
    }

    return true;
}

bool CheckInt(std::string &val) {
    if (val[0] != '-' && !isdigit(val[0])) { return false; }
    for (int i = 1; i < val.size(); ++i) {
        if (!isdigit(val[i])) {
            return false;
        }
    }
    return true;
}

bool CheckDecimal(std::string &val) {
    if (val[0] != '-' && !isdigit(val[0])) { return false; }
    int i = val[0] == '-' ? 1 : 0;
    int dot_pos = val.find_first_of('.');
    if (dot_pos == std::string::npos) {
        while (i < val.size()) {
            if (!isdigit(val[i++])) { return false; }
        }
    } else {
        while (i < dot_pos) {
            if (!isdigit(val[i++])) { return false; }
        }
        while (++i < val.size()) {
            if (!isdigit(val[i])) { return false; }
        }
    }
    return true;
}

std::string CheckField(std::string &fld, const std::string &col_def) {
    if (col_def.find("datetime") != std::string::npos) {
        if (CheckDateTime(fld)) { return fld; }
		return std::move("2020-04-01 00:00:00.0");
    } else if (col_def.find("int") != std::string::npos) {
        if (!CheckInt(fld)) { return "0"; }
		return std::move(fld);
    } else if (col_def.find("char") != std::string::npos) {
		int idx = 0;
		idx = col_def.find('(', idx) + 1;
		int n = std::stoi(col_def.substr(idx, col_def.size() - idx - 1));
		if (fld.size() > n) {
			fld = std::move(fld.substr(0, n));
		}
		return std::move(fld);
    } else if (col_def.find("decimal") != std::string::npos){
        if (!CheckDecimal(fld)) { return "0"; }
		int dot_idx = col_def.find(',', 0);
		int right_brac_idx = col_def.find(')', 0);
		int n = std::stoi(col_def.substr(dot_idx + 1, right_brac_idx - dot_idx - 1));
        char val[32] = {'\0'};
        if (fld[0] == '-') {
            sprintf(val, "%.*lf", n, std::stod(fld) - 1e-10);
        } else {
            sprintf(val, "%.*lf", n, std::stod(fld) + 1e-10);
        }
        return std::move(std::string(val));
    } else {
        return std::move(fld);
    }
}

// Line records verified and in the format file_no|pos \\t primary key \\t origin fields
// line: string , line record
// tblnam2idx: map from table name to table index in vector
// tbl_idx[out_param]:
// tbl_metas: meta info of each table
// src_file_no: source file no of this thread
// line_pos: the position in the file of the line
// line_rec_meta[out_param]: the file no and position in source file of the line
void LineVerify(char *line, std::unordered_map<std::string, int> &tblname2idx, int &tbl_idx, \
                        std::vector<struct table_t> &tbl_metas, int src_file_no, int64_t line_pos, \
                        std::pair<uint64_t, struct record_meta_t> &line_rec_meta) {
    // const int pk_field_setw = 12;
    std::vector<std::string> line_vec;
    int prev_pos = -1;
    int fld_len = 0;
    fld_len = strcspn(line + prev_pos + 1, "\t");
    do {
        line_vec.emplace_back(std::move(std::string(line + prev_pos + 1, fld_len)));
        prev_pos += fld_len + 1;
        fld_len = strcspn(line + prev_pos + 1, "\t");
    } while (fld_len > 0);
    std::string tbl_name = line_vec[2];
    tbl_idx = tblname2idx.at(tbl_name);

    line_vec.erase(line_vec.begin(), line_vec.begin() + 3);
    auto&& tbl_meta = tbl_metas[tbl_idx];
    std::vector<int> pk_col_idxs;
    for (auto& idx : tbl_meta.indexs_) {
        if (idx.primary_) {
            for (auto& index_col_idx : idx.index_cols_) {
                pk_col_idxs.emplace_back(index_col_idx);
            }
        }
    }
    // std::stringstream pk_str;
    // for (auto &idx : pk_col_idxs) {
    //     pk_str << std::setw(pk_field_setw) << std::setfill('0') << line_vec[idx] << "|";
    // }
    int occupy_bitcnt = 0;
    uint64_t pk = 0;
    for (auto &idx : pk_col_idxs) {
        uint64_t fld_val = std::stoul(line_vec[idx]);
        if (tbl_meta.colunms_[idx].col_def_.find("tiny") != std::string::npos) {
            pk |= fld_val << (64 - 8 - occupy_bitcnt);
            occupy_bitcnt += 8;
        } else if (tbl_meta.colunms_[idx].col_def_.find("small") != std::string::npos) {
            pk |= fld_val << (64 - 16 - occupy_bitcnt);
            occupy_bitcnt += 16;
        } else {
            pk |= fld_val << (64 - 32 - occupy_bitcnt);
            occupy_bitcnt += 32;
        }
    }
    struct record_meta_t line_info;
    line_info.file_no_ = src_file_no;
    line_info.pos_ = line_pos;
    line_rec_meta = std::make_pair(pk, std::move(line_info));
}

void SrcRoutine(std::string &src_file_path, \
            std::vector<std::mutex> &mut_tbls, std::unordered_map<std::string, int> &tblname2idx,\
            std::vector<struct table_t> &tbl_metas, std::vector<std::fstream> &table_tmp_files) {
    std::fstream fs;
    const int buf_size = 1024;
    int src_file_no = std::stoi(src_file_path.substr(src_file_path.find_last_of('_') + 1));
    char *buf = new char[buf_size];
    fs.open(src_file_path, std::fstream::in);
    int64_t line_pos = 0;
    char buf_line_info[24];
    while (!fs.eof()) {
        bzero(buf, buf_size);
        fs.getline(buf, buf_size);
        int tbl_idx = 0;
        std::pair<uint64_t, struct record_meta_t> line_info;
        LineVerify(buf, tblname2idx, tbl_idx, tbl_metas, src_file_no, line_pos, line_info);
        // Send to target table thread by queue
        bzero(buf_line_info, 24);
        *((uint64_t*)(buf_line_info)) = line_info.first;
        *((uint64_t*)(buf_line_info + 8)) = line_info.second.file_no_;
        *((uint64_t*)(buf_line_info + 16)) = line_info.second.pos_;
        mut_tbls[tbl_idx].lock();
        table_tmp_files[tbl_idx].write(buf_line_info, 24);
        // table_tmp_files[tbl_idx] << line_info.first << "\t" << line_info.second.file_no_ << "\t" << line_info.second.pos_ << std::endl;
        mut_tbls[tbl_idx].unlock();
        line_pos = fs.tellg();
    }
    fs.close();
    delete[] buf;
}

void TableRoutine(std::fstream &tmp_fs, struct table_t &meta,\
                    std::vector<std::fstream> &fs_vec,const std::string &output_dir) {
    std::map<uint64_t, struct record_meta_t> pk2lineinfo;
    const int buf_size = 1024;
    char *buf = new char[buf_size];
    bzero(buf, buf_size);
    std::cout << "Loading meta line records ..." << std::endl;
    // Load from tmp_fs file
    tmp_fs.seekg(0, tmp_fs.beg);
    uint64_t pk;
    struct record_meta_t rec_meta;
    char buf_rec_meta[24];
    while (!tmp_fs.eof()) {
        bzero(buf_rec_meta, 24);
        tmp_fs.read(buf_rec_meta, 24);
        pk = *((uint64_t*)(buf_rec_meta));
        rec_meta.file_no_ = *((uint64_t*)(buf_rec_meta + 8));
        rec_meta.pos_ = *((uint64_t*)(buf_rec_meta + 16));
        // tmp_fs >> pk >> rec_meta.file_no_ >> rec_meta.pos_;
        // Using invalid file_no value (file_no == 0) check read end of line
        if (rec_meta.file_no_ == 0) { continue; }
        if (pk2lineinfo.find(pk) == pk2lineinfo.end()) {
            pk2lineinfo.emplace(pk, rec_meta);
        } else {
            auto&& old_ele = pk2lineinfo.at(pk);
            if (rec_meta.file_no_ > old_ele.file_no_ || \
                (rec_meta.file_no_ == old_ele.file_no_ && rec_meta.pos_ > old_ele.pos_)) {
                pk2lineinfo.erase(pk);
                pk2lineinfo.emplace(pk, rec_meta);
            }
        }
    }

    tmp_fs.close();
    // Load from src file by lines records, verify and write to output file
    const std::string& table_name = meta.table_name_;
    std::fstream out_fs;
    out_fs.open(output_dir+"/tianchi_dts_sink_data_"+table_name, std::fstream::out);
    int rec_count = pk2lineinfo.size();
    std::cout << "Loading finish." << std::endl;
    auto result_io_routine = [&fs_vec, buf, buf_size, &meta, &out_fs](std::pair<const uint64_t, record_meta_t> &ele)->void {
        // Load from src file
        fs_vec[ele.second.file_no_].seekg(ele.second.pos_);
        bzero(buf, buf_size);
        fs_vec[ele.second.file_no_].getline(buf, buf_size);
        std::vector<std::string> line_vec;
        int prev_pos = -1;
        int fld_len = 0;
        fld_len = strcspn(buf + prev_pos + 1, "\t");
        do {
            line_vec.emplace_back(std::move(std::string(buf + prev_pos + 1, fld_len)));
            prev_pos += fld_len + 1;
            fld_len = strcspn(buf + prev_pos + 1, "\t");
        } while (fld_len > 0);
        line_vec.erase(line_vec.begin(), line_vec.begin() + 3);
        // Verify and write
        line_vec[0] = std::move(CheckField(line_vec[0], meta.colunms_[0].col_def_));
        out_fs << line_vec[0];
        for (int i = 1; i < line_vec.size(); ++i) {
            line_vec[i] = std::move(CheckField(line_vec[i], meta.colunms_[i].col_def_));
            out_fs << "\t" << line_vec[i];
        }
    };
    
    std::cout << "Start write, line info vec size=" << rec_count << std::endl;
    for (auto &ele : pk2lineinfo) {
        result_io_routine(ele);
        if (__glibc_likely(--rec_count > 0)) {
            out_fs << "\n";
        }
    }

    std::cout << "Write records finished." << std::endl;
    out_fs.close();
    delete[] buf;
}


int main(int argc, char const *argv[]) {
    const std::string meta_file = std::string(argv[1]) + "/schema_info_dir/schema.info";
    const std::string src_dir = std::string(argv[1]) + "/source_file_dir";
    const std::string output_dir = std::string(argv[2]) + "/sink_file_dir";
    // Directory for storing middle result from src threads
    const std::string record_line_tmp_fdir = std::string(argv[2]);

    std::vector<std::string>&& src_file_paths = GetSrcFiles(src_dir);
    std::vector<struct table_t>&& tables = LoadTableMeta(meta_file);
    std::unordered_map<std::string, int> tblname2idx;
    for (int i = 0; i < tables.size(); ++i) {
        tblname2idx.insert(std::make_pair(tables[i].table_name_, i));
    }
    std::vector<std::mutex> mut_tbls(tables.size());
    std::vector<std::fstream> table_tmp_files(tables.size());
    // Open table tmp files
    for (int i = 0; i < tables.size(); ++i) {
        table_tmp_files[i].open(record_line_tmp_fdir + "/tmp_" + tables[i].table_name_, std::fstream::out | std::fstream::binary);
    }

    std::vector<std::thread> src_ths;
    for (int i = 0; i < src_file_paths.size(); ++i) {
        std::thread th(SrcRoutine, std::ref(src_file_paths[i]), std::ref(mut_tbls), std::ref(tblname2idx), std::ref(tables), std::ref(table_tmp_files));
        src_ths.emplace_back(std::move(th));
    }
    for (int i = 0; i < src_file_paths.size(); ++i) {
        src_ths[i].join();
    }

    time_t tm_stmp = std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
    std::cout << "All src thread routine finish, tm=" << std::ctime(&tm_stmp) << std::endl;
    std::vector<std::fstream> fs_vec(src_file_paths.size() + 1);
    // Open infstream of source files
    for (int i = 1; i < fs_vec.size(); ++i) {
        fs_vec[i].open(src_file_paths[i - 1], std::fstream::in);
    }
    for (auto &tmpf : table_tmp_files) {
        tmpf.close();
    }
    // Open table tmp files
    for (int i = 0; i < tables.size(); ++i) {
        table_tmp_files[i].open(record_line_tmp_fdir + "/tmp_" + tables[i].table_name_, std::fstream::in | std::fstream::binary);
    }
    for (int i = 0; i < tables.size(); ++i) {
        std::cout << "Processing table #" << i << " ..." << std::endl;
        TableRoutine(table_tmp_files[i], tables[i], fs_vec, output_dir);;
    }
    for (int i = 1; i < fs_vec.size(); ++i) {
        fs_vec[i].close();
    }
    return 0;
}
