#/bin/bash

workload="workloads/workloada.spec"
dbpath="/home/denghejian/WTtest"
moreworkloads="workloads/workloada.spec:workloads/workloadb.spec:workloads/workloadc.spec:workloads/workloadd.spec:workloads/workloade.spec:workloads/workloadf.spec"

if [ -n "$dbpath" ];then
    rm -rf $dbpath/*
fi

# cmd="./ycsbc -db rocksdb -dbpath $dbpath -threads 4 -P $workload -load true -morerun $moreworkloads -dbstatistics true "

cmd="./ycsbc -db wiredtiger -threads 4 -P $workload -load true -morerun $moreworkloads -dbstatistics true "

if [ -n "$1" ];then    #后台运行
cmd="nohup ./ycsbc -db wiredtiger -dbpath $dbpath -threads 32 -P $workload -load true -morerun $moreworkloads -dbstatistics true >>$1 2>&1  &"
echo $cmd >$1
fi

echo $cmd
eval $cmd
