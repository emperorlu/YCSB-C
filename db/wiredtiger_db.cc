//
// Created by wujy on 1/23/19.
//
#include <iostream>
#include <sstream>

#include "wiredtiger_db.h"
#include "lib/coding.h"
using namespace std;

namespace ycsbc {
    WiredTiger::WiredTiger(const char *home, utils::Properties &props) :noResult(0){
        WT_SESSION *session;
        //set option
        /* Open a connection to the database, creating it if necessary. */
        wiredtiger_open(home, NULL, SetConnOptions(props).c_str(), &conn_);
        assert(conn_ != NULL);

        /* Open a session handle for the database. */
        conn_->open_session(conn_, NULL, NULL, &session);
        assert(session != NULL);

        int ret = session->create(session, uri_.c_str(), SetCreateOptions(props).c_str());
        if (ret != 0) {
            fprintf(stderr, "create error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        session->close(session, NULL);

        session_nums_ = stoi(props.GetProperty("threadcount", "1"));
        session_ = new WT_SESSION *[session_nums_];
        for (int i = 0; i < session_nums_; i++){
            conn_->open_session(conn_, NULL, NULL, &(session_[i]));
            assert(session_[i] != NULL);
        }
    }

    std::string WiredTiger::SetConnOptions(utils::Properties &props) {
        //
        uint64_t nums = stoi(props.GetProperty(CoreWorkload::RECORD_COUNT_PROPERTY));
        uint32_t key_len = stoi(props.GetProperty(CoreWorkload::KEY_LENGTH));
        uint32_t value_len = stoi(props.GetProperty(CoreWorkload::FIELD_LENGTH_PROPERTY));
        uint32_t cache_size = nums * (key_len + value_len) * 10 / 100;

        std::stringstream conn_config;
        conn_config.str("");

        conn_config << "create";

        // string direct_io = ",direct_io=[data]"; // [data, log, checkpoint]
        // conn_config += direct_io;

        conn_config << ",cache_size=";
        conn_config << cache_size;

        // string checkpoint = ",checkpoint=(wait=60,log_size=2G)";
        // conn_config += checkpoint;

        // string extensions = ",extensions=[/usr/local/lib/libwiredtiger_snappy.so]";
        // conn_config += extensions;

        // string log = ",log=(archive=false,enabled=true,path=journal,compressor=snappy)"; // compressor = "lz4", "snappy", "zlib" or "zstd" // 需要重新Configuring WiredTiger
        conn_config << "log=(enabled=true,file_max=128MB)";

        conn_config << ",statistics=(fast)"; // all, fast
        cout << "Connect Config: " << conn_config.str() << endl;
        return conn_config.str();
    }

    std::string WiredTiger::SetCreateOptions(utils::Properties &props) {
        //
        uri_ = "table:ycsb_wiredtiger" ; //"lsm:ycsb_wiredtiger"
        std::stringstream config;
        config.str("");

        config << "key_format=S,value_format=S";
        config << ",prefix_compression=false";
        config << ",checksum=off";
        
        config << ",internal_page_max=16kb";
        config << ",leaf_page_max=16kb";
        config << ",memory_page_max=100MB";

        // config << ",lsm=(";
        // config << ",chunk_size=20MB";
        // config << ",bloom_bit_count=10"
        // config << ")";

        cout << "Create Config: " << config.str() << endl;
        return config.str();
    }

    // int WiredTiger::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
    //                   std::vector<KVPair> &result) {return DB::kOK;}
    int WiredTiger::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result, int nums) {
        WT_CURSOR *cursor;
        int ret = session_[nums]->open_cursor(session_[nums], uri_.c_str(), NULL, NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        cursor->set_key(cursor, key.c_str());
        ret = cursor->search(cursor);
        cursor->close(cursor);
        if(!ret){
            return DB::kOK;
        }else if(ret == WT_NOTFOUND){
            noResult++;
            return DB::kOK;
        }else {
            cerr<<"read error"<<endl;
            exit(0);
        }
    }

    // int WiredTiger::Scan(const std::string &table, const std::string &key, const std::string &max_key, int len, const std::vector<std::string> *fields,
    //                   std::vector<std::vector<KVPair>> &result) {return DB::kOK;}
    int WiredTiger::Scan(const std::string &table, const std::string &key, const std::string &max_key, int len, const std::vector<std::string> *fields,
                      std::vector<std::vector<KVPair>> &result, int nums) {
        WT_CURSOR *cursor;
        int ret = session_[nums]->open_cursor(session_[nums], uri_.c_str(), NULL, NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        cursor->set_key(cursor, key.c_str());
        int i = 0;
        while (i < len && cursor->next(cursor) == 0) {
            i++;
            const char *key1;
            cursor->get_key(cursor, &key1);
            string key2 = key1;
            if(key2 >= key) break;
        }
        cursor->close(cursor);
        return DB::kOK;
    }

    // int WiredTiger::Insert(const std::string &table, const std::string &key,
    //                     std::vector<KVPair> &values) {return DB::kOK;}
    int WiredTiger::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values, int nums){
        WT_CURSOR *cursor;
        int ret = session_[nums]->open_cursor(session_[nums], uri_.c_str(), NULL, NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }

        string value = values.at(0).second;

        cursor->set_key(cursor, key.c_str());
        cursor->set_value(cursor, value.c_str());
        ret = cursor->insert(cursor);
        if (ret != 0) {
          fprintf(stderr, "set error: %s\n", wiredtiger_strerror(ret));
          exit(1);
        }
        cursor->close(cursor);
        return DB::kOK;
    }

    // int WiredTiger::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
    //     return Insert(table,key,values);
    // }
    int WiredTiger::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values, int nums) {
        return Insert(table,key,values,nums);
    }

    // int WiredTiger::Delete(const std::string &table, const std::string &key) {return DB::kOK;}
    int WiredTiger::Delete(const std::string &table, const std::string &key, int nums) {
        WT_CURSOR *cursor;
        int ret = session_[nums]->open_cursor(session_[nums], uri_.c_str(), NULL, NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        cursor->set_key(cursor, key.c_str());
        if ((ret = cursor->remove(cursor)) != 0) {
          if (nums == 1 || ret != WT_NOTFOUND) {
            fprintf(stderr, "del error: key %s %s\n", key.c_str(), wiredtiger_strerror(ret));
            exit(1);
          }
        }
        cursor->close(cursor);
        return DB::kOK;
    }

    inline void print_cursor(WT_CURSOR *cursor)
    {
        const char *desc, *pvalue;
        int64_t value;

        while (cursor->next(cursor) == 0 &&
            cursor->get_value(cursor, &desc, &pvalue, &value) == 0){
            if (value != 0)
                printf("%s=%s\n", desc, pvalue);
        }
    }

    void WiredTiger::PrintStats() {
        if(noResult) cout<<"read not found:"<<noResult<<endl;
        char stats[1024];
        memset(stats, 0, 1024);
        // statistics must be set
        WT_CURSOR *cursor;
        WT_SESSION *session;
        std::stringstream suri;
        suri.str("");
        suri << "statistics:" << uri_;
        conn_->open_session(conn_, NULL, NULL, &session);
        int ret = session->open_cursor(session, suri.str().c_str(), NULL, NULL, &cursor);
        if (ret != 0) {
            fprintf(stderr, "open_cursor error: %s\n", wiredtiger_strerror(ret));
            exit(1);
        }
        print_cursor(cursor);
        session->close(session, NULL);
        cout<<stats<<endl;
    }


    WiredTiger::~WiredTiger() {
        for(int i = 0; i < session_nums_; i++) {
            session_[i]->close(session_[i], NULL);
        }
        conn_->close(conn_, NULL);
    }

    void WiredTiger::SerializeValues(std::vector<KVPair> &kvs, std::string &value) {
        value.clear();
        PutFixed64(&value, kvs.size());
        for(unsigned int i=0; i < kvs.size(); i++){
            PutFixed64(&value, kvs[i].first.size());
            value.append(kvs[i].first);
            PutFixed64(&value, kvs[i].second.size());
            value.append(kvs[i].second);
        }
    }

    void WiredTiger::DeSerializeValues(std::string &value, std::vector<KVPair> &kvs){
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
