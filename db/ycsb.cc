#include "ycsb.h"

////statistics
atomic<uint64_t> ops_cnt[ycsbc::Operation::READMODIFYWRITE + 1];    //操作个数
atomic<uint64_t> ops_time[ycsbc::Operation::READMODIFYWRITE + 1];   //微秒
////

void SetProps(utils::Properties &props) {
  props.SetProperty("threadcount", "1");
  props.SetProperty("dbname", "pelagodb");
  props.SetProperty("load", "true");
  props.SetProperty("morerun", "/home/hhs/workloads/test_workloada.spec");
  // props.SetProperty("dbstatistics", "true");
  ifstream input("/home/hhs/workloads/test_workloada.spec");
  try {
    props.Load(input);
  } catch (const string &message) {
    cout << message << endl;
    exit(0);
  }
  input.close();
}

string ParseCommandLine(int argc, const char *argv[], utils::Properties &props) {
  int argindex = 3;  // TODO : kvlog read yscb xxx
  string filename;
  while (argindex < argc && StrStartWith(argv[argindex], "-")) {
    if (strcmp(argv[argindex], "-threads") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("threadcount", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-db") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbname", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-host") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("host", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-port") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("port", argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-slaves") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("slaves", argv[argindex]);
      argindex++;
    } else if(strcmp(argv[argindex],"-dbpath")==0){
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbpath", argv[argindex]);
      argindex++;
    } else if(strcmp(argv[argindex],"-load")==0){
      argindex++;
      if(argindex >= argc){
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("load",argv[argindex]);
      argindex++;
    } else if(strcmp(argv[argindex],"-run")==0){
      argindex++;
      if(argindex >= argc){
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("run",argv[argindex]);
      argindex++;
    } else if(strcmp(argv[argindex],"-dboption")==0){
      argindex++;
      if(argindex >= argc){
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dboption",argv[argindex]);
      argindex++;
    } else if(strcmp(argv[argindex],"-dbstatistics")==0){
      argindex++;
      if(argindex >= argc){
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbstatistics",argv[argindex]);
      argindex++;
    } else if(strcmp(argv[argindex],"-dbwaitforbalance")==0){
      argindex++;
      if(argindex >= argc){
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("dbwaitforbalance",argv[argindex]);
      argindex++;
    } else if(strcmp(argv[argindex],"-morerun")==0){
      argindex++;
      if(argindex >= argc){
        UsageMessage(argv[0]);
        exit(0);
      }
      props.SetProperty("morerun",argv[argindex]);
      argindex++;
    } else if (strcmp(argv[argindex], "-P") == 0) {
      argindex++;
      if (argindex >= argc) {
        UsageMessage(argv[0]);
        exit(0);
      }
      filename.assign(argv[argindex]);
      ifstream input(argv[argindex]);
      try {
        props.Load(input);
      } catch (const string &message) {
        cout << message << endl;
        exit(0);
      }
      input.close();
      argindex++;
    } else {
      cout << "Unknown option '" << argv[argindex] << "'" << endl;
      exit(0);
    }
  }

  if (argindex == 1 || argindex != argc) {
    UsageMessage(argv[0]);
    exit(0);
  }

  return filename;
}

void UsageMessage(const char *command) {
  cout << "Usage: " << command << " [options]" << endl;
  cout << "Options:" << endl;
  cout << "  -threads n: execute using n threads (default: 1)" << endl;
  cout << "  -db dbname: specify the name of the DB to use (default: basic)" << endl;
  cout << "  -P propertyfile: load properties from the given file. Multiple files can" << endl;
  cout << "                   be specified, and will be processed in the order specified" << endl;
}

inline bool StrStartWith(const char *str, const char *pre) {
  return strncmp(str, pre, strlen(pre)) == 0;
}

void Init(utils::Properties &props){
  props.SetProperty("dbname","basic");
  props.SetProperty("dbpath","");
  props.SetProperty("load","false");
  props.SetProperty("run","false");
  props.SetProperty("threadcount","1");
  props.SetProperty("dboption","0");
  props.SetProperty("dbstatistics","false");
  props.SetProperty("dbwaitforbalance","false");
  props.SetProperty("morerun","");
}

void PrintInfo(utils::Properties &props) {
  printf("---- dbname:%s  dbpath:%s ----\n", props["dbname"].c_str(), props["dbpath"].c_str());
  printf("%s", props.DebugString().c_str());
  printf("----------------------------------------\n");
  fflush(stdout);
}