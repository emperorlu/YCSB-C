#ifndef YCSB_H
#define YCSB_H

#include <cstring>
#include <string>
#include <iostream>
#include <vector>
#include <future>
#include <unistd.h>
#include <atomic>
#include "core/utils.h"
#include "core/timer.h"
#include "core/client.h"
#include "core/core_workload.h"
#include "db/db_factory.h"

using namespace std;

void UsageMessage(const char *command);
bool StrStartWith(const char *str, const char *pre);
string ParseCommandLine(int argc, const char *argv[], utils::Properties &props);
void Init(utils::Properties &props);
void PrintInfo(utils::Properties &props);

int DelegateClient(ycsbc::DB *db, ycsbc::CoreWorkload *wl, const int num_ops,
    bool is_loading, int nums);

#endif