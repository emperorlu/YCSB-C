//
// 
//

#ifndef YCSB_C_PELAGODB_H
#define YCSB_C_PELAGODB_H

#include "../leveldb/include/leveldb/pelagodb_c.h"


#include "core/db.h"
#include <iostream>
#include <string>
#include "core/properties.h"
#include "core/core_workload.h"


using std::cout;
using std::endl;

namespace ycsbc {
    class PELAGODB : public DB{
    public :
        PELAGODB(const char *dbfilename, utils::Properties &props);
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

        void Close();
        
        ~PELAGODB();

    private:
        pelagodb_t *db_;
        unsigned noResult;

        void SerializeValues(std::vector<KVPair> &kvs, std::string &value);
        void DeSerializeValues(std::string &value, std::vector<KVPair> &kvs);

        

    };
}


#endif //YCSB_C_ROCKSDB_DB_H
