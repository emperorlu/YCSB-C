#include "ycsb_c.h"

extern "C" {

int YCSB_TEST( const int argc, const char *argv[]) {
  utils::Properties props;
  Init(props);
  string file_name = ParseCommandLine(argc, argv, props);

  ycsbc::DB *db = ycsbc::DBFactory::CreateDB(props);
  if (!db) {
    cout << "Unknown database name " << props["dbname"] << endl;
    exit(0);
  }

  const bool load = utils::StrToBool(props.GetProperty("load","false"));
  const bool run = utils::StrToBool(props.GetProperty("run","false"));
  const int num_threads = stoi(props.GetProperty("threadcount", "1"));
  const bool print_stats = utils::StrToBool(props["dbstatistics"]);
  const bool wait_for_balance = utils::StrToBool(props["dbwaitforbalance"]);

  string morerun = props["morerun"];

  vector<future<int>> actual_ops;
  int total_ops = 0;
  int sum = 0;
  utils::Timer<double> timer;

  PrintInfo(props);

  if( load ) {
    // Loads data
    ycsbc::CoreWorkload wl;
    wl.Init(props);

    uint64_t load_start = get_now_micros();
    total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);
    for (int i = 0; i < num_threads; ++i) {
      actual_ops.emplace_back(async(launch::async,
          DelegateClient, db, &wl, total_ops / num_threads, true, i));
    }
    assert((int)actual_ops.size() == num_threads);

    sum = 0;
    for (auto &n : actual_ops) {
      assert(n.valid());
      sum += n.get();
    }
    uint64_t load_end = get_now_micros();
    uint64_t use_time = load_end - load_start;
    printf("********** load result **********\n");
    printf("loading records:%d  use time:%.3f s  IOPS:%.2f iops (%.2f us/op)\n", sum, 1.0 * use_time*1e-6, 1.0 * sum * 1e6 / use_time, 1.0 * use_time / sum);
    printf("*********************************\n");

    if ( print_stats ) {
        printf("-------------- db statistics --------------\n");
        db->PrintStats();
        printf("-------------------------------------------\n");
    }
  } 
  if( run ) {
    // Peforms transactions
    ycsbc::CoreWorkload wl;
    wl.Init(props);

    for(int j = 0; j < ycsbc::Operation::READMODIFYWRITE + 1; j++){
      ops_cnt[j].store(0);
      ops_time[j].store(0);
    }

    actual_ops.clear();
    total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
    uint64_t run_start = get_now_micros();
    for (int i = 0; i < num_threads; ++i) {
      actual_ops.emplace_back(async(launch::async,
          DelegateClient, db, &wl, total_ops / num_threads, false, i));
    }
    assert((int)actual_ops.size() == num_threads);
    sum = 0;
    for (auto &n : actual_ops) {
      assert(n.valid());
      sum += n.get();
    }
    uint64_t run_end = get_now_micros();
    uint64_t use_time = run_end - run_start;

    uint64_t temp_cnt[ycsbc::Operation::READMODIFYWRITE + 1];
    uint64_t temp_time[ycsbc::Operation::READMODIFYWRITE + 1];

    for(int j = 0; j < ycsbc::Operation::READMODIFYWRITE + 1; j++){
      temp_cnt[j] = ops_cnt[j].load(std::memory_order_relaxed);
      temp_time[j] = ops_time[j].load(std::memory_order_relaxed);
    }

    printf("********** run result **********\n");
    printf("all opeartion records:%d  use time:%.3f s  IOPS:%.2f iops (%.2f us/op)\n\n", sum, 1.0 * use_time*1e-6, 1.0 * sum * 1e6 / use_time, 1.0 * use_time / sum);
    if ( temp_cnt[ycsbc::INSERT] )          printf("insert ops:%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::INSERT], 1.0 * temp_time[ycsbc::INSERT]*1e-6, 1.0 * temp_cnt[ycsbc::INSERT] * 1e6 / temp_time[ycsbc::INSERT], 1.0 * temp_time[ycsbc::INSERT] / temp_cnt[ycsbc::INSERT]);
    if ( temp_cnt[ycsbc::READ] )            printf("read ops  :%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::READ], 1.0 * temp_time[ycsbc::READ]*1e-6, 1.0 * temp_cnt[ycsbc::READ] * 1e6 / temp_time[ycsbc::READ], 1.0 * temp_time[ycsbc::READ] / temp_cnt[ycsbc::READ]);
    if ( temp_cnt[ycsbc::UPDATE] )          printf("update ops:%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::UPDATE], 1.0 * temp_time[ycsbc::UPDATE]*1e-6, 1.0 * temp_cnt[ycsbc::UPDATE] * 1e6 / temp_time[ycsbc::UPDATE], 1.0 * temp_time[ycsbc::UPDATE] / temp_cnt[ycsbc::UPDATE]);
    if ( temp_cnt[ycsbc::SCAN] )            printf("scan ops  :%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::SCAN], 1.0 * temp_time[ycsbc::SCAN]*1e-6, 1.0 * temp_cnt[ycsbc::SCAN] * 1e6 / temp_time[ycsbc::SCAN], 1.0 * temp_time[ycsbc::SCAN] / temp_cnt[ycsbc::SCAN]);
    if ( temp_cnt[ycsbc::READMODIFYWRITE] ) printf("rmw ops   :%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::READMODIFYWRITE], 1.0 * temp_time[ycsbc::READMODIFYWRITE]*1e-6, 1.0 * temp_cnt[ycsbc::READMODIFYWRITE] * 1e6 / temp_time[ycsbc::READMODIFYWRITE], 1.0 * temp_time[ycsbc::READMODIFYWRITE] / temp_cnt[ycsbc::READMODIFYWRITE]);
    printf("********************************\n");
  }
  if( !morerun.empty() ) {
    vector<string> runfilenames;
    size_t start=0,index=morerun.find_first_of(':', 0);
    while(index!=morerun.npos)
    {
        if(start!=index)
            runfilenames.push_back(morerun.substr(start,index-start));
        start=index+1;
        index=morerun.find_first_of(':',start);
    }
    if(!morerun.substr(start).empty()) {
      runfilenames.push_back(morerun.substr(start));
    }
    for(unsigned int i = 0; i < runfilenames.size(); i++){
      for(int j = 0; j < ycsbc::Operation::READMODIFYWRITE + 1; j++){
        ops_cnt[j].store(0);
        ops_time[j].store(0);
      }

      ifstream input(runfilenames[i]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      printf("------ run:%s ------\n",runfilenames[i].c_str());
      PrintInfo(props);
      // Peforms transactions
      ycsbc::CoreWorkload wl;
      wl.Init(props);

      actual_ops.clear();
      total_ops = stoi(props[ycsbc::CoreWorkload::OPERATION_COUNT_PROPERTY]);
      uint64_t run_start = get_now_micros();
      for (int i = 0; i < num_threads; ++i) {
        actual_ops.emplace_back(async(launch::async,
            DelegateClient, db, &wl, total_ops / num_threads, false, i));
      }
      assert((int)actual_ops.size() == num_threads);
      sum = 0;
      for (auto &n : actual_ops) {
        assert(n.valid());
        sum += n.get();
      }
      uint64_t run_end = get_now_micros();
      uint64_t use_time = run_end - run_start;

      uint64_t temp_cnt[ycsbc::Operation::READMODIFYWRITE + 1];
      uint64_t temp_time[ycsbc::Operation::READMODIFYWRITE + 1];

      for(int j = 0; j < ycsbc::Operation::READMODIFYWRITE + 1; j++){
        temp_cnt[j] = ops_cnt[j].load(std::memory_order_relaxed);
        temp_time[j] = ops_time[j].load(std::memory_order_relaxed);
      }

      printf("********** more run result **********\n");
      printf("all opeartion records:%d  use time:%.3f s  IOPS:%.2f iops (%.2f us/op)\n\n", sum, 1.0 * use_time*1e-6, 1.0 * sum * 1e6 / use_time, 1.0 * use_time / sum);
      if ( temp_cnt[ycsbc::INSERT] )          printf("insert ops:%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::INSERT], 1.0 * temp_time[ycsbc::INSERT]*1e-6, 1.0 * temp_cnt[ycsbc::INSERT] * 1e6 / temp_time[ycsbc::INSERT], 1.0 * temp_time[ycsbc::INSERT] / temp_cnt[ycsbc::INSERT]);
      if ( temp_cnt[ycsbc::READ] )            printf("read ops  :%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::READ], 1.0 * temp_time[ycsbc::READ]*1e-6, 1.0 * temp_cnt[ycsbc::READ] * 1e6 / temp_time[ycsbc::READ], 1.0 * temp_time[ycsbc::READ] / temp_cnt[ycsbc::READ]);
      if ( temp_cnt[ycsbc::UPDATE] )          printf("update ops:%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::UPDATE], 1.0 * temp_time[ycsbc::UPDATE]*1e-6, 1.0 * temp_cnt[ycsbc::UPDATE] * 1e6 / temp_time[ycsbc::UPDATE], 1.0 * temp_time[ycsbc::UPDATE] / temp_cnt[ycsbc::UPDATE]);
      if ( temp_cnt[ycsbc::SCAN] )            printf("scan ops  :%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::SCAN], 1.0 * temp_time[ycsbc::SCAN]*1e-6, 1.0 * temp_cnt[ycsbc::SCAN] * 1e6 / temp_time[ycsbc::SCAN], 1.0 * temp_time[ycsbc::SCAN] / temp_cnt[ycsbc::SCAN]);
      if ( temp_cnt[ycsbc::READMODIFYWRITE] ) printf("rmw ops   :%7lu  use time:%7.3f s  IOPS:%7.2f iops (%.2f us/op)\n", temp_cnt[ycsbc::READMODIFYWRITE], 1.0 * temp_time[ycsbc::READMODIFYWRITE]*1e-6, 1.0 * temp_cnt[ycsbc::READMODIFYWRITE] * 1e6 / temp_time[ycsbc::READMODIFYWRITE], 1.0 * temp_time[ycsbc::READMODIFYWRITE] / temp_cnt[ycsbc::READMODIFYWRITE]);
      printf("********************************\n");

      if ( print_stats ) {
        printf("-------------- db statistics --------------\n");
        db->PrintStats();
        printf("-------------------------------------------\n");
      }

    }
    
  }
  if ( print_stats ) {
    printf("-------------- db statistics --------------\n");
    db->PrintStats();
    printf("-------------------------------------------\n");
  }
  if ( wait_for_balance ) {
    uint64_t sleep_time = 0;
    while(!db->HaveBalancedDistribution()){
      sleep(10);
      sleep_time += 10;
    }
    printf("Wait balance:%lu s\n",sleep_time);

    printf("-------------- db statistics --------------\n");
    db->PrintStats();
    printf("-------------------------------------------\n");
  }
  delete db;
  return 0;
}





    
}