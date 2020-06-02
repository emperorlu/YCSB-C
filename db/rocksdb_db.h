//
// 
//

#ifndef YCSB_C_ROCKSDB_DB_H
#define YCSB_C_ROCKSDB_DB_H

#include "core/db.h"
#include <iostream>
#include <string>
#include "core/properties.h"
#include <rocksdb/db.h>

using std::cout;
using std::endl;

namespace ycsbc {
    class RocksDB : public DB{
    public :
        RocksDB(const char *dbfilename, utils::Properties &props);
        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result, int nums);

        int Scan(const std::string &table, const std::string &key, const std::string &max_key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result, int nums);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values, int nums);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values, int nums);


        int Delete(const std::string &table, const std::string &key, int nums);

        void PrintStats();

        ~RocksDB();

    private:
        rocksdb::DB *db_;
        unsigned noResult;

        void SetOptions(rocksdb::Options *options, utils::Properties &props);
        void SerializeValues(std::vector<KVPair> &kvs, std::string &value);
        void DeSerializeValues(std::string &value, std::vector<KVPair> &kvs);

    };
}


#endif //YCSB_C_ROCKSDB_DB_H
