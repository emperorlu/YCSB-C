//
// Created by wujy on 1/23/19.
//
#include <iostream>


#include "wiredtiger_db.h"
#include "lib/coding.h"
using namespace std;

namespace ycsbc {
    void (*custom_die)(void) = NULL;
    const char *progname = "program name not set";

    /*
    * testutil_die --
    *     Report an error and abort.
    */
    void testutil_die(int e, const char *fmt, ...)
    {
        va_list ap;

        /* Flush output to be sure it doesn't mix with fatal errors. */
        (void)fflush(stdout);
        (void)fflush(stderr);

        /* Allow test programs to cleanup on fatal error. */
        if (custom_die != NULL)
            (*custom_die)();

        fprintf(stderr, "%s: FAILED", progname);
        if (fmt != NULL) {
            fprintf(stderr, ": ");
            va_start(ap, fmt);
            vfprintf(stderr, fmt, ap);
            va_end(ap);
        }
        if (e != 0)
            fprintf(stderr, ": %s", wiredtiger_strerror(e));
        fprintf(stderr, "\n");
        fprintf(stderr, "process aborting\n");

        abort();
    }

    WiredTiger::WiredTiger(const char *home, utils::Properties &props) :noResult(0){
        WT_SESSION *session;
        //set option
        const char *CONN_CONFIG = SetConnOptions(props).c_str();
            // "create,cache_size=100MB,direct_io=[data],log=(archive=false,enabled=true)";
        /* Open a connection to the database, creating it if necessary. */
        error_check(wiredtiger_open(home, NULL, CONN_CONFIG, &conn_));

        /* Open a session handle for the database. */
        error_check(conn_->open_session(conn_, NULL, NULL, &session));

        const char *CONFIG = SetOptions(props).c_str()
        error_check(session->create(session, uri_.c_str(), CONFIG));

        // error_check(session->open_cursor(session, uri, NULL, NULL, &cursor));
        error_check(session->close(session, NULL));

        session_nums_ = stoi(props.GetProperty("threadcount", "1"));
        session_ = new (WT_SESSION *)[session_nums_];
        for (int i = 0; i < session_nums_; i++){
            error_check(conn_->open_session(conn_, NULL, NULL, &(session[i]));
            assert(session[i] != NULL);
        }
    }

    std::string WiredTiger::SetConnOptions(utils::Properties &props) {
        //
        std::string conn_config = "" ;

        conn_config += "create";

        // string direct_io = ",direct_io=[data]"; // [data, log, checkpoint]
        // conn_config += direct_io;

        conn_config += ",cache_size=128MB,";

        // string checkpoint = ",checkpoint=(wait=60,log_size=2G)";
        // conn_config += checkpoint;

        // string extensions = ",extensions=[/usr/local/lib/libwiredtiger_snappy.so]";
        // conn_config += extensions;

        // string log = ",log=(archive=false,enabled=true,path=journal,compressor=snappy)"; // compressor = "lz4", "snappy", "zlib" or "zstd" // 需要重新Configuring WiredTiger
        // conn_config += log;

        conn_config += ",statistics=(fast)"; // all, fast

        return conn_config;
    }

    std::string WiredTiger::SetOptions(utils::Properties &props) {
        //
        uri_ = "table:ycsb_wiredtiger" ; //"lsm:ycsb_wiredtiger"
        std::stringstream config ;
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
        
        return config.str();
    }

    // int WiredTiger::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
    //                   std::vector<KVPair> &result) {return DB::kOK;}
    int WiredTiger::Read(const std::string &table, const std::string &key, const std::vector<std::string> *fields,
                      std::vector<KVPair> &result, int nums) {
        WT_CURSOR *cursor;
        error_check(session_[nums]->open_cursor(session_[nums], uri_.c_str(), NULL, NULL, &cursor));
        cursor->set_key(cursor, key.c_str());
        int ret = cursor->search(cursor);
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
        error_check(session_[nums]->open_cursor(session_[nums], uri_.c_str(), NULL, NULL, &cursor));
        cursor->set_key(cursor, key.c_str());
        int i = 0, ret;
        while (i < len && (ret = cursor->next(cursor)) == 0) {
            i++;
            const char *key1;
            error_check(cursor->get_key(cursor, &key1));
            string key2 = key1;
            if(key2 >= key) break;
        }
        return DB::kOK;
    }

    // int WiredTiger::Insert(const std::string &table, const std::string &key,
    //                     std::vector<KVPair> &values) {return DB::kOK;}
    int WiredTiger::Insert(const std::string &table, const std::string &key,
                        std::vector<KVPair> &values, int nums){
        WT_CURSOR *cursor;
        error_check(session_[nums]->open_cursor(session_[nums], uri_.c_str(), NULL, NULL, &cursor));

        string value = values.at(0).second;

        cursor->set_key(cursor, key.c_str());
        cursor->set_value(cursor, value.c_str());
        error_check(cursor->insert(cursor));
        
        return DB::kOK;
    }

    // int WiredTiger::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values) {
    //     return Insert(table,key,values);
    // }
    int WiredTiger::Update(const std::string &table, const std::string &key, std::vector<KVPair> &values, int nums) {
        return Insert(table,key,values,nums);
    }

    // int WiredTiger::Delete(const std::string &table, const std::string &key) {return DB::kOK;}
    int WiredTiger::Delete(const std::string &table, const std::string &key) {
        WT_CURSOR *cursor;
        error_check(session_[nums]->open_cursor(session_[nums], uri_.c_str(), NULL, NULL, &cursor));
        cursor->set_key(cursor, key.c_str());
        error_check(cursor->remove(cursor));
        return DB::kOK;
    }

    inline void print_cursor(WT_CURSOR *cursor)
    {
        const char *desc, *pvalue;
        int64_t value;
        int ret;

        while ((ret = cursor->next(cursor)) == 0) {
            error_check(cursor->get_value(cursor, &desc, &pvalue, &value));
            if (value != 0)
                printf("%s=%s\n", desc, pvalue);
        }
        scan_end_check(ret == WT_NOTFOUND);
    }

    void WiredTiger::PrintStats() {
        if(noResult) cout<<"read not found:"<<noResult<<endl;
        char stats[1024];
        memset(stats, 0, 1024);
        // statistics must be set
        WT_CURSOR *cursor;
        WT_SESSION *session;
        error_check(conn_->open_session(conn_, NULL, NULL, &session));
        error_check(session->open_cursor(session, "statistics:table:test", NULL, NULL, &cursor));
        print_cursor(cursor);
        error_check(session->close(session, NULL));
        cout<<stats<<endl;
    }


    WiredTiger::~WiredTiger() {
        for(int i = 0; i < session_nums_; i++) {
            error_check(session_[i]->close(session_[i], NULL));
        }
        delete session_;
        error_check(conn_->close(conn_, NULL));
        delete conn_;
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
