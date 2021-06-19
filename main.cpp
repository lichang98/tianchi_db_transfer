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
#include <thread>
#include <queue>
#include <mutex>
#include <unordered_set>


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
    return index;
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
    return table;
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
    return tables;
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
    if (val.size() > 21) { return false; }
    if (!isdigit(val[0]) || !isdigit(val[1]) || !isdigit(val[2]) || !isdigit(val[3]) \
        ||!isdigit(val[5]) || !isdigit(val[6]) || !isdigit(val[8]) || !isdigit(val[9])\
        || !isdigit(val[11]) || !isdigit(val[12]) || !isdigit(val[14]) || !isdigit(val[15])\
        || !isdigit(val[17]) || !isdigit(val[18]) || !isdigit(val[20])) {
            return false;
    }
    if (val[4] != '-' || val[7] != '-') { return false; }
    if (val[13] != ':' || val[16] != ':' || val[19] != '.') { return false; }
    return true;
}

bool CheckInt(std::string &val) {
    for (auto& ch : val) {
        if (!isdigit(ch)) {
            return false;
        }
    }
    return true;
}

bool CheckDecimal(std::string &val) {
    int dot_pos = val.find_first_of('.');
    if (dot_pos == val.npos) {
        return false;
    }
    int val_size = val.size();
    for (int i = 0; i < dot_pos; ++i) {
        if (!isdigit(val[i])) { return false; }
    }
    for (int i = dot_pos + 1; i < val_size; ++i) {
        if (!isdigit(val[i])) { return false; }
    }
    return true;
}

std::string CheckField(std::string &fld, const std::string &col_def) {
    if (col_def.find("datetime") != col_def.npos) {
        if (CheckDateTime(fld)) { return fld; }
		return "2020-04-01 00:00:00.0";
    } else if (col_def.find("int") != col_def.npos) {
        if (!CheckInt(fld)) {
			return "0";
		}
		int idx = 0;
		idx = col_def.find('(', idx) + 1;
		int n = std::stoi(col_def.substr(idx, col_def.size() - idx - 1));
		if (fld.size() > n) {
			fld = fld.substr(fld.size() - n);
		}
		return fld;
    } else if (col_def.find("char") != col_def.npos) {
		int idx = 0;
		idx = col_def.find('(', idx) + 1;
		int n = std::stoi(col_def.substr(idx, col_def.size() - idx - 1));
		if (fld.size() > n) {
			fld = fld.substr(0, n);
		}
		return fld;
    } else if (col_def.find("text") != col_def.npos) {
        return fld;
    } else {
        if (!CheckDecimal(fld)) {
			return "0";
		}
		int dot_idx = col_def.find(',', 0);
		int right_brac_idx = col_def.find(')', 0);
		int n = std::stoi(col_def.substr(dot_idx + 1, right_brac_idx - dot_idx - 1));
        char val[32] = {'\0'};
        sprintf(val, "%.*lf", n, std::stod(fld));
        return std::string(val);
    }
}

// Line records verified and in the format file_no|pos \t primary key \t origin fields
std::vector<std::string> LineVerify(char *line, std::unordered_map<std::string, int> &tblname2idx, int &tbl_idx, \
                        std::vector<struct table_t> &tbl_metas, int src_file_no, int64_t line_pos) {
    std::vector<std::string> line_vec;
    int prev_pos = -1;
    int fld_len = 0;
    fld_len = strcspn(line + prev_pos + 1, "\t");
    do {
        line_vec.emplace_back(std::string(line + prev_pos + 1, fld_len));
        prev_pos += fld_len + 1;
        fld_len = strcspn(line + prev_pos + 1, "\t");
    } while (fld_len > 0);
    std::string tbl_name = line_vec[2];
    tbl_idx = tblname2idx.at(tbl_name);

    line_vec.erase(line_vec.begin(), line_vec.begin() + 3);
    auto&& col_meta = tbl_metas[tbl_idx].colunms_;
    // // Verify fields
    for (int i = 0; i < line_vec.size(); ++i) {
        line_vec[i] = std::move(CheckField(line_vec[i], col_meta[i].col_def_));
    }
    // Insert file_no&pos and primary key
    auto&& tbl_meta = tbl_metas[tbl_idx];
    std::vector<int> pk_col_idxs;
    for (auto& idx : tbl_meta.indexs_) {
        if (idx.primary_) {
            for (auto& index_col_idx : idx.index_cols_) {
                pk_col_idxs.emplace_back(index_col_idx);
            }
        }
    }
    std::string pk_str;
    for (auto& idx : pk_col_idxs) {
        pk_str += line_vec[idx] + "|";
    }
    // Insert primary key into line_vec
    line_vec.insert(line_vec.begin(), pk_str);
    line_vec.insert(line_vec.begin(), std::to_string(line_pos));
    line_vec.insert(line_vec.begin(), std::to_string(src_file_no));
    return line_vec;
}

void SrcRoutine(std::vector<std::fstream> &fs_tmp_tbls, std::string &src_file_path, \
            std::vector<std::mutex> &mut_tbls, std::unordered_map<std::string, int> &tblname2idx,\
            std::vector<struct table_t> &tbl_metas) {
    std::fstream fs;
    const int buf_size = 1024;
    int src_file_no = std::stoi(src_file_path.substr(src_file_path.find_last_of('_') + 1));
    char *buf = new char[buf_size];
    fs.open(src_file_path, std::fstream::in);
    int64_t line_pos = 0;
    while (!fs.eof()) {
        bzero(buf, buf_size);
        fs.getline(buf, buf_size);
        int tbl_idx = 0;
        std::vector<std::string>&& line_verified = LineVerify(buf, tblname2idx, tbl_idx, tbl_metas, src_file_no, line_pos);
        // Write verified line record to target table's file
        mut_tbls[tbl_idx].lock();
        int i = 0;
        fs_tmp_tbls[tbl_idx] << line_verified[i++];
        while (i < line_verified.size()) {
            fs_tmp_tbls[tbl_idx] << "\t" << line_verified[i++];
        }
        fs_tmp_tbls[tbl_idx] << "\n";
        mut_tbls[tbl_idx].unlock();
        line_pos = fs.tellg();
    }
    fs.close();
}


int main(int argc, char const *argv[]) {
    const std::string meta_file = std::string(argv[1]) + "/schema_info_dir/schema.info";
    const std::string src_dir = std::string(argv[1]) + "/source_file_dir";
    const std::string output_dir = std::string(argv[2]) + "/sink_file_dir";

    std::vector<std::string>&& src_file_paths = GetSrcFiles(src_dir);
    std::vector<struct table_t>&& tables = LoadTableMeta(meta_file);
    std::unordered_map<std::string, int> tblname2idx;
    for (int i = 0; i < tables.size(); ++i) {
        tblname2idx.insert(std::make_pair(tables[i].table_name_, i));
    }
    std::vector<std::mutex> mut_tbls(tables.size());
    std::vector<std::fstream> fs_tmp_tbls;
    for (int i = 0; i < tables.size(); ++i) {
        std::fstream fs;
        fs.open("tmp"+std::to_string(i)+".txt", std::fstream::out);
        fs_tmp_tbls.emplace_back(std::move(fs));
    }

    SrcRoutine(fs_tmp_tbls, src_file_paths[0], mut_tbls, tblname2idx,tables);

    for (int i = 0; i < tables.size(); ++i) {
        fs_tmp_tbls[i].close();
    }
    // Load records from src files, verify and write to output files





    // std::string str = "I\ttianchi_dts_data\torders\t5\t1\t1\t1517\t2021-04-11 15:41:55.0\t9\t6\t1";
    // int pos = -1;
    // int fields_len = 0;
    // for (int i = 0; i < 14; ++i) {
    //     fields_len = strcspn(str.c_str() + pos + 1, "\t");
    //     pos += fields_len + 1;
    //     std::cout << "pos=" << pos << " fields len=" << fields_len << std::endl;
    // }
    

    // double a=12.4488123;
    // char buf[32]={'\0'};
    // sprintf(buf, "%*.*lf",1,2 ,a);
    // printf("val=%s\n", buf);


    // int fd = open("tmp.txt", O_RDONLY, 0);
    // if (fd == -1) {
    //     perror("File not found!");
    //     return -1;
    // }
    // char *buf = static_cast<char*>(mmap64(NULL, 1, PROT_READ, MAP_PRIVATE|MAP_POPULATE, fd, 0));
    // if (buf == MAP_FAILED) {
    //     perror("Map file failed!");
    //     return -1;
    // }
    // fprintf(stdout, "%s\n", buf);
    // int str_len = strcspn(buf, "\n");
    // printf("pos=%d\n", str_len);
    // char *line = buf;
    // printf("get line=%s\n", line);
    // int old_pos = str_len + 1;

    // for (int i = 0; i < 36; ++i) {
    //     str_len = strcspn(buf + old_pos, "\n");
    //     printf("pos=%d\n", str_len);
    //     line = buf + old_pos;
    //     printf("get line=%s\n", line);
    //     old_pos += str_len + 1;
    // }
    return 0;
}
