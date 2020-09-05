#include "ycsb_c.h"

#include "db/ycsb.h"
#include "lib/lwt_lock.h"

extern "C" {
typedef struct _SharedState {
  lock_t lock;
  cond_t cond;
  int total;
  int num_done;
  int oks_sum;
} SharedState;

typedef struct _ThreadState {
  ycsbc::DB *db;
  ycsbc::CoreWorkload *wl;
  int num_ops;
  bool is_loading;
  int nums;
} ThreadState;

typedef struct _ThreadArg {
  SharedState* shared;
  ThreadState* thread;
  int(*method)(ThreadState*);
} ThreadArg;

SharedState *CreatSharedState(int num) {
  SharedState *temp = (SharedState *)malloc(sizeof(SharedState));
  LockInit(&temp->lock);
  CondInit(&temp->cond, &temp->lock);
  temp->total = num;
  temp->num_done = 0;
  
  return temp;
}

void DeleteSharedState(SharedState **sh) {
  SharedState *temp = *sh;
  LockFree(&temp->lock);
  CondFree(&temp->cond);
  free(temp);
  *sh = NULL;
}

ThreadState *CreatThreadState(int id) {
  ThreadState *temp = (ThreadState *)malloc(sizeof(ThreadState));
  temp->db = nullptr_t;
  temp->wl = nullptr_t;
  temp->nums = id;

  return temp;
}

void DeleteThreadState(ThreadState **ts){
  free(*ts);
  *ts = NULL;
}

void ThreadBody(void *v) {
  ThreadArg *arg = (ThreadArg *)(v);
  SharedState *shared = arg->shared;
  ThreadState *thread = arg->thread;

  int oks = (arg->method)(thread);

  Lock(&shared->lock);
  shared->oks_sum += oks;
  shared->num_done++;
  if(shared->num_done >= shared->total){
    SignalAll(&shared->cond);
  }
  Unlock(&shared->lock);
}

int DelegateClient(ThreadState *thread) {
  ycsbc::DB *db = thread->db;
  ycsbc::CoreWorkload *wl = thread->wl;
  int num_ops = thread->num_ops;
  bool is_loading = thread->num_ops;
  int nums = thread->nums;
  
  db->Init();
  ycsbc::Client client(*db, *wl, nums);
  int oks = 0;
  int next_report_ = 0;
  for (int i = 0; i < num_ops; ++i) {

    if (i >= next_report_) {
        if      (next_report_ < 1000)   next_report_ += 100;
        else if (next_report_ < 5000)   next_report_ += 500;
        else if (next_report_ < 10000)  next_report_ += 1000;
        else if (next_report_ < 50000)  next_report_ += 5000;
        else if (next_report_ < 100000) next_report_ += 10000;
        else if (next_report_ < 500000) next_report_ += 50000;
        else                            next_report_ += 100000;
        fprintf(stderr, "... finished %d ops%30s\r", i, "");
        fflush(stderr);
    }
    if (is_loading) {
      oks += client.DoInsert();
    } else {
      oks += client.DoTransaction();
    }
  }
  db->Close();
  return oks;
}

int YCSB_TEST() {
  utils::Properties props;
  Init(props);
  // string file_name = ParseCommandLine(argc, argv, props);
  SetProps(props);
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

    uint64_t load_start = ycsb_get_now_micros();
    total_ops = stoi(props[ycsbc::CoreWorkload::RECORD_COUNT_PROPERTY]);

    SharedState *shared = CreatSharedState(num_threads);
    ThreadArg *arg = (ThreadArg *)malloc(sizeof(ThreadArg) * num_threads);
    for (int i = 0; i < num_threads; ++i) {
      // actual_ops.emplace_back(async(launch::async,
      //     DelegateClient, db, &wl, total_ops / num_threads, true, i));
      arg[i].shared = shared;
      arg[i].thread = CreatThreadState(i);
      arg[i].thread->is_loading = true;
      arg[i].thread->db = db;
      arg[i].thread->num_ops = total_ops / num_threads;
      arg[i].method = DelegateClient;
      thread_add_task(&ThreadBody, &arg[i], 1, 0);
    }
    // assert((int)actual_ops.size() == num_threads);

    Lock(&shared->lock);
    while(shared->num_done < num_threads){
      Wait(&shared->cond, &shared->lock);
    }
    sum = shared->oks_sum;
    Unlock(&shared->lock);

    // sum = 0;
    // for (auto &n : actual_ops) {
    //   assert(n.valid());
    //   sum += n.get();
    // }
    uint64_t load_end = ycsb_get_now_micros();
    uint64_t use_time = load_end - load_start;

    for(int i = 0; i < num_threads; ++i){
      DeleteThreadState(&arg[i].thread);
    }
    DeleteSharedState(&shared);
    free(arg);

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
    uint64_t run_start = ycsb_get_now_micros();
    
    SharedState *shared = CreatSharedState(num_threads);
    ThreadArg *arg = (ThreadArg *)malloc(sizeof(ThreadArg) * num_threads);
    for (int i = 0; i < num_threads; ++i) {
      // actual_ops.emplace_back(async(launch::async,
      //     DelegateClient, db, &wl, total_ops / num_threads, true, i));
      arg[i].shared = shared;
      arg[i].thread = CreatThreadState(i);
      arg[i].thread->is_loading = false;
      arg[i].thread->db = db;
      arg[i].thread->num_ops = total_ops / num_threads;
      arg[i].method = DelegateClient;
      thread_add_task(&ThreadBody, &arg[i], 1, 0);
    }
    // assert((int)actual_ops.size() == num_threads);

    Lock(&shared->lock);
    while(shared->num_done < num_threads){
      Wait(&shared->cond, &shared->lock);
    }
    sum = shared->oks_sum;
    Unlock(&shared->lock);

    uint64_t run_end = ycsb_get_now_micros();
    uint64_t use_time = run_end - run_start;

    for(int i = 0; i < num_threads; ++i){
      DeleteThreadState(&arg[i].thread);
    }
    DeleteSharedState(&shared);
    free(arg);

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
      uint64_t run_start = ycsb_get_now_micros();
      
      SharedState *shared = CreatSharedState(num_threads);
      ThreadArg *arg = (ThreadArg *)malloc(sizeof(ThreadArg) * num_threads);
      for (int i = 0; i < num_threads; ++i) {
        // actual_ops.emplace_back(async(launch::async,
        //     DelegateClient, db, &wl, total_ops / num_threads, true, i));
        arg[i].shared = shared;
        arg[i].thread = CreatThreadState(i);
        arg[i].thread->is_loading = false;
        arg[i].thread->db = db;
        arg[i].thread->num_ops = total_ops / num_threads;
        arg[i].method = DelegateClient;
        thread_add_task(&ThreadBody, &arg[i], 1, 0);
      }
      // assert((int)actual_ops.size() == num_threads);

      Lock(&shared->lock);
      while(shared->num_done < num_threads){
        Wait(&shared->cond, &shared->lock);
      }
      sum = shared->oks_sum;
      Unlock(&shared->lock);

      uint64_t run_end = ycsb_get_now_micros();
      uint64_t use_time = run_end - run_start;

      for(int i = 0; i < num_threads; ++i){
        DeleteThreadState(&arg[i].thread);
      }
      DeleteSharedState(&shared);
      free(arg);

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