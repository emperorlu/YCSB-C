//
// Created by wujy on 1/23/19.
//
#include <iostream>


#include "pelagodb_db.h"
#include "lib/coding.h"

using namespace std;

namespace ycsbc {

    struct Data_S{   //适合pelagodb的字符串
        char *data_;  //data前四个字节是size_,然后才是数据

        Data_S():data_(nullptr) {};

        Data_S(const char *str){
            uint32_t size_ = strlen(str);
            data_ = new char[size_];
            memcpy(data_, str, size_);
        }

        Data_S(const std::string &str){
            uint32_t size_ = str.size();
            data_ = new char[size_];
            memcpy(data_, str.c_str(), size_);
        }

        ~Data_S(){
            if(data_ != nullptr)  delete []data_;
        }

        char *data(){
            return data_;
        }

        uint32_t size(){
            return strlen(data_);
        }

    };

    PELAGODB::PELAGODB(const char *dbfilename, utils::Properties &props) :noResult(0){
        //set option
        pelagodb_options_t *op = pelagodb_options_create();
        if(NULL == op) assert(0);
        pelagodb_options_set_bind_core_id(op, 9);
        char *err = NULL;
        db_ = pelagodb_open(op, "ycsb_pelagodb", &err);
        if(NULL == db_) {
            cerr<<"Can't open PELAGODB "<<dbfilename<<endl;
            pelagodb_options_destroy(op);
            assert(0);
        }
    }

    int PELAGODB::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result, int nums) {
        pelagodb_readoptions_t* roptions = NULL;
        roptions = pelagodb_readoptions_create();
        if(NULL == roptions){
            cerr<<"Alloc roptions failed"<<endl;
            assert(0);
        }
        uint32_t snapid = 0;
        uint32_t snapfound = 0;
        uint32_t value_len = 0;
        char *err = NULL;
        Data_S find_key(key);
        char *value = nullptr;
        value = pelagodb_get(db_, roptions, find_key.data(), key.size(), snapid, &snapfound, (size_t *)(&value_len), &err);
        if(NULL != err) {
            cerr<<"query Key fail, key = "<<key<<endl;
            free(err);
            assert(0);
        }
        else if (nullptr == value) {
            noResult++;
        } 
        pelagodb_readoptions_destroy(roptions);
        free(value);
        return DB::kOK;
    }


    int PELAGODB::Scan(const std::string &table, const std::string &key, const std::string &max_key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result, int nums) {
        pelagodb_readoptions_t* roptions = NULL;
        pelagodb_iterator_t *iter = NULL;
        roptions = pelagodb_readoptions_create();
        if(NULL == roptions){
            cerr<<"Alloc roptions failed"<<endl;
            assert(0);
        }
        iter = pelagodb_iterator_create(db_, roptions);
        if(iter == NULL){
            cerr<<"New iterator failed"<<endl;
            assert(0);
        }
        int i;
        for(i = 0; i < len && pelagodb_iterator_valid(iter); i++){
            pelagodb_iterator_key(iter);
            pelagodb_iterator_value(iter);
            pelagodb_iterator_next(iter);
        } 
        pelagodb_iterator_destroy(iter);
        pelagodb_readoptions_destroy(roptions);
        return DB::kOK;
    }

    int PELAGODB::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values, int nums){
        char *err = NULL;
        string value = values.at(0).second;
        uint32_t snapid = 0;
        pelagodb_writeoptions_t *woptions = NULL;
        pelagodb_writebatch_t *wb = NULL;
    
        woptions = pelagodb_writeoptions_create();
        if(NULL == woptions) {
            cerr<<"Alloc woptions failed"<<endl;
            assert(0);
        }
        pelagodb_writeoptions_set_sync(woptions, 1);
        wb = pelagodb_writebatch_create();
        if(NULL == wb) {
            cerr<<"Alloc wb failed"<<endl;
            assert(0);
        }
        pelagodb_writebatch_put(wb, Data_S(key).data(), key.size(), snapid, Data_S(value).data(), value.size());
        pelagodb_write(db_, woptions, wb, &err);
        if(NULL != err){
            cerr<<"insert error"<<endl;
            free(err);
            assert(0);
        }   
        pelagodb_writeoptions_destroy(woptions);
        pelagodb_writebatch_destroy(wb);
        return DB::kOK;
    }

    int PELAGODB::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values, int nums) {
        return Insert(table,key,values,nums);
    }

    int PELAGODB::Delete(const std::string &table, const std::string &key, int nums) {
        char *err = NULL;
        uint32_t snapid = 0;
        pelagodb_writeoptions_t *woptions = NULL;
        pelagodb_writebatch_t *wb = NULL;
    
        woptions = pelagodb_writeoptions_create();
        if(NULL == woptions) {
            cerr<<"Alloc woptions failed"<<endl;
            assert(0);
        }
        pelagodb_writeoptions_set_sync(woptions, 1);
        wb = pelagodb_writebatch_create();
        if(NULL == wb) {
            cerr<<"Alloc wb failed"<<endl;
            assert(0);
        }
        pelagodb_writebatch_delete(wb, Data_S(key).data(), key.size(), snapid);
        pelagodb_write(db_, woptions, wb, &err);
        if(NULL != err){
            cerr<<"delete error"<<endl;
            free(err);
            assert(0);
        }   
        return DB::kOK;
    }

    void PELAGODB::PrintStats() {
        if(noResult) cout<<"read not found:"<<noResult<<endl;
        // char stats[4096];
        // memset(stats, 0, 4096);
        // db_->interface.PrintStats(db_->db, (char *)&stats);
        // cout<<stats<<endl;
    }

    void PELAGODB::Close() {
        pelagodb_init_stats(db_);
    }

    PELAGODB::~PELAGODB() {
        if(db_) pelagodb_close(db_);
    }

    void PELAGODB::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    void PELAGODB::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
        uint64_t offset = 0;
        uint64_t kv_num = 0;
        uint64_t key_size = 0;
        uint64_t value_size = 0;

        kv_num = DecodeFixed64(value.c_str());
        offset += 8;
        for( unsigned int i = 0; i < kv_num; i++){
            ycsbc::DB::KVPair pair;
            key_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.first.assign(value.c_str() + offset, key_size);
            offset += key_size;

            value_size = DecodeFixed64(value.c_str() + offset);
            offset += 8;

            pair.second.assign(value.c_str() + offset, value_size);
            offset += value_size;
            kvs.push_back(pair);
        }
    }
}
