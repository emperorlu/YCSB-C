#/bin/bash

workload="workloads/test_workloada.spec"
dbpath="/home/hhs/WTtest"
moreworkloads="workloads/test_workloada.spec:workloads/test_workloadb.spec:workloads/test_workloadc.spec:workloads/test_workloadd.spec:workloads/test_workloade.spec:workloads/test_workloadf.spec"

if [ -n "$dbpath" ];then
    rm -rf $dbpath/*
fi

# cmd="./ycsbc -db rocksdb -dbpath $dbpath -threads 4 -P $workload -load true -morerun $moreworkloads -dbstatistics true "

cmd="./ycsbc -db wiredtiger -dbpath $dbpath -threads 32 -P $workload -load true -dbstatistics true "

if [ -n "$1" ];then    #后台运行
cmd="nohup ./ycsbc -db wiredtiger -dbpath $dbpath -threads 32 -P $workload -load true -morerun $moreworkloads -dbstatistics true >>$1 2>&1  &"
echo $cmd >$1
fi

echo $cmd
eval $cmd