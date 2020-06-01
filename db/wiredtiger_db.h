//
// 
//

#ifndef YCSB_C_WIREDTIGER_DB_H
#define YCSB_C_WIREDTIGER_DB_H

#include "core/db.h"
#include <iostream>
#include <string>
#include "core/properties.h"
#include <wiredtiger/wiredtiger_ext.h>

using std::cout;
using std::endl;

#define error_check(call)                                              \
    do {                                                               \
        int __r;                                                       \
        if ((__r = (call)) != 0 && __r != ENOTSUP)                     \
            testutil_die(__r, "%s/%d: %s", __func__, __LINE__, #call); \
    } while (0)

#define testutil_assert(a)                                        \
    do {                                                          \
        if (!(a))                                                 \
            testutil_die(0, "%s/%d: %s", __func__, __LINE__, #a); \
    } while (0)

#define scan_end_check(a) testutil_assert(a)

namespace ycsbc {
    class WiredTiger : public DB{
    public :
        WiredTiger(const char *home, utils::Properties &props);
        int Read(const std::string &table, const std::string &key,
                 const std::vector<std::string> *fields,
                 std::vector<KVPair> &result);

        int Scan(const std::string &table, const std::string &key, const std::string &max_key,
                 int len, const std::vector<std::string> *fields,
                 std::vector<std::vector<KVPair>> &result);

        int Insert(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);

        int Update(const std::string &table, const std::string &key,
                   std::vector<KVPair> &values);


        int Delete(const std::string &table, const std::string &key);

        void PrintStats();

        ~WiredTiger();

    private:
        WT_CONNECTION *conn_;
        std::string uri_;
        unsigned noResult;

        std::string SetConnOptions(utils::Properties &props);
        std::string SetSessionOptions(utils::Properties &props);
        void SerializeValues(std::vector<KVPair> &kvs, std::string &value);
        void DeSerializeValues(std::string &value, std::vector<KVPair> &kvs);

    };
}


#endif //YCSB_C_ROCKSDB_DB_H
