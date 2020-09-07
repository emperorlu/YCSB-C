//
//  client.h
//  YCSB-C
//
//  Created by Jinglei Ren on 12/10/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#ifndef YCSB_C_CLIENT_H_
#define YCSB_C_CLIENT_H_


#include <string>
#include "lib/atomic.h"
#include "lib/histogram.h"
#include "db.h"
#include "core_workload.h"
#include "utils.h"

using namespace std;

extern atomic_uint64_t ops_cnt[ycsbc::Operation::READMODIFYWRITE + 1] ;    //操作个数
extern atomic_uint64_t ops_time[ycsbc::Operation::READMODIFYWRITE + 1] ;   //微秒
extern HistogramImpl hist_time;

namespace ycsbc {

class Client {
 public:
  Client(DB &db, CoreWorkload &wl, int nums) : db_(db), workload_(wl), nums_(nums){ }
  
  virtual bool DoInsert();
  virtual bool DoTransaction();
  
  virtual ~Client() { }
  
 protected:
  
  virtual int TransactionRead();
  virtual int TransactionReadModifyWrite();
  virtual int TransactionScan();
  virtual int TransactionUpdate();
  virtual int TransactionInsert();
  
  DB &db_;
  CoreWorkload &workload_;
  int nums_;
};

inline bool Client::DoInsert() {
  std::string key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> pairs;
  workload_.BuildValues(pairs);
  return (db_.Insert(workload_.NextTable(), key, pairs, nums_) == DB::kOK);
}

inline bool Client::DoTransaction() {
  int status = -1;
  uint64_t start_time = ycsb_get_now_micros();
  uint64_t end_time = 0;
  switch (workload_.NextOperation()) {
    case READ:
      status = TransactionRead();
      end_time = ycsb_get_now_micros();
      atomic_fetch_add_explicit(&ops_time[READ], (end_time - start_time ), memory_order_relaxed);
      atomic_fetch_add_explicit(&ops_cnt[READ], 1, memory_order_relaxed);
      hist_time.interface.Add(&hist_time, end_time - start_time);
      // ops_time[READ].fetch_add((ycsb_get_now_micros() - start_time ), std::memory_order_relaxed);
      // ops_cnt[READ].fetch_add(1, std::memory_order_relaxed);
      break;
    case UPDATE:
      status = TransactionUpdate();
      end_time = ycsb_get_now_micros();
      atomic_fetch_add_explicit(&ops_time[UPDATE], (end_time - start_time ), memory_order_relaxed);
      atomic_fetch_add_explicit(&ops_cnt[UPDATE], 1, memory_order_relaxed);
      hist_time.interface.Add(&hist_time, end_time - start_time);
      // ops_time[UPDATE].fetch_add((ycsb_get_now_micros() - start_time ), std::memory_order_relaxed);
      // ops_cnt[UPDATE].fetch_add(1, std::memory_order_relaxed);
      break;
    case INSERT:
      status = TransactionInsert();
      end_time = ycsb_get_now_micros();
      atomic_fetch_add_explicit(&ops_time[INSERT], (end_time - start_time ), memory_order_relaxed);
      atomic_fetch_add_explicit(&ops_cnt[INSERT], 1, memory_order_relaxed);
      hist_time.interface.Add(&hist_time, end_time - start_time);
      // ops_time[INSERT].fetch_add((ycsb_get_now_micros() - start_time ), std::memory_order_relaxed);
      // ops_cnt[INSERT].fetch_add(1, std::memory_order_relaxed);
      break;
    case SCAN:
      status = TransactionScan();
      end_time = ycsb_get_now_micros();
      atomic_fetch_add_explicit(&ops_time[SCAN], (end_time - start_time ), memory_order_relaxed);
      atomic_fetch_add_explicit(&ops_cnt[SCAN], 1, memory_order_relaxed);
      hist_time.interface.Add(&hist_time, end_time - start_time);
      // ops_time[SCAN].fetch_add((ycsb_get_now_micros() - start_time ), std::memory_order_relaxed);
      // ops_cnt[SCAN].fetch_add(1, std::memory_order_relaxed);
      break;
    case READMODIFYWRITE:
      status = TransactionReadModifyWrite();
      end_time = ycsb_get_now_micros();
      atomic_fetch_add_explicit(&ops_time[READMODIFYWRITE], (end_time - start_time ), memory_order_relaxed);
      atomic_fetch_add_explicit(&ops_cnt[READMODIFYWRITE], 1, memory_order_relaxed);
      hist_time.interface.Add(&hist_time, end_time - start_time);
      // ops_time[READMODIFYWRITE].fetch_add((ycsb_get_now_micros() - start_time ), std::memory_order_relaxed);
      // ops_cnt[READMODIFYWRITE].fetch_add(1, std::memory_order_relaxed);
      break;
    default:
      throw utils::Exception("Operation request is not recognized!");
  }
  assert(status >= 0);
  return (status == DB::kOK);
}

inline int Client::TransactionRead() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Read(table, key, &fields, result, nums_);
  } else {
    return db_.Read(table, key, NULL, result, nums_);
  }
}

inline int Client::TransactionReadModifyWrite() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> result;

  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    db_.Read(table, key, &fields, result, nums_);
  } else {
    db_.Read(table, key, NULL, result, nums_);
  }

  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values, nums_);
}

inline int Client::TransactionScan() {
  const std::string &table = workload_.NextTable();
  //const std::string &key = workload_.NextTransactionKey();
  //const std::string &max_key = workload_.BuildMaxKey();
  std::string key;
  std::string max_key;
  workload_.NextTransactionScanKey(key, max_key);
  int len = workload_.NextScanLength();
  std::vector<std::vector<DB::KVPair>> result;
  if (!workload_.read_all_fields()) {
    std::vector<std::string> fields;
    fields.push_back("field" + workload_.NextFieldName());
    return db_.Scan(table, key, max_key, len, &fields, result, nums_);
  } else {
    return db_.Scan(table, key, max_key, len, NULL, result, nums_);
  }
}

inline int Client::TransactionUpdate() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextTransactionKey();
  std::vector<DB::KVPair> values;
  if (workload_.write_all_fields()) {
    workload_.BuildValues(values);
  } else {
    workload_.BuildUpdate(values);
  }
  return db_.Update(table, key, values, nums_);
}

inline int Client::TransactionInsert() {
  const std::string &table = workload_.NextTable();
  const std::string &key = workload_.NextSequenceKey();
  std::vector<DB::KVPair> values;
  workload_.BuildValues(values);
  return db_.Insert(table, key, values, nums_);
} 

} // ycsbc

#endif // YCSB_C_CLIENT_H_
