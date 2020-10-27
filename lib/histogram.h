//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef YCSB_C_HISTOGRAM_H
#define YCSB_C_HISTOGRAM_H

#include <math.h>
#include <stdbool.h>
#include <cstdint>
#include <atomic>
// #include "lock.h"

using namespace std;

typedef void* OBJ;

#define MAX_MAP_SIZE 200

typedef struct {
    uint64_t bucketValues_[MAX_MAP_SIZE];
    uint64_t size_;
    uint64_t maxBucketValue_;
    uint64_t minBucketValue_;
} HistogramBucketMapper;

int CreateHistogramBucketMapper(HistogramBucketMapper* mapper);
void DeleteHistogramBucketMapper(HistogramBucketMapper* mapper);

typedef struct {
    HistogramBucketMapper bucketMapper_;

    atomic<uint64_t> min_;
    atomic<uint64_t> max_;
    atomic<uint64_t> num_;
    atomic<uint64_t> sum_;
    atomic<uint64_t> sum_squares_;
    atomic<uint64_t> buckets_[MAX_MAP_SIZE]; // 109==BucketMapper::BucketCount()
    uint64_t num_buckets_;
} HistogramStat;

int CreateHistogramStat(HistogramStat* stat);
void DeleteHistogramStat(HistogramStat* stat);

typedef struct {
    void (*Clear)(OBJ impl);
    int (*Empty)(OBJ impl);
    void (*Add)(OBJ impl, uint64_t value);
    void (*Merge)(OBJ impl, OBJ other);
    char* (*ToString)(OBJ impl);
    double (*Median)(OBJ impl);
    double (*Percentile)(OBJ impl, double p);
    double (*Average)(OBJ impl);
    double (*StandardDeviation)(OBJ impl);
} HistogramInterface;

typedef struct{
    bool init_;
    HistogramStat stats_;
    // lock_t lock_;
    HistogramInterface interface;
} HistogramImpl;

int CreateHistogramImpl(HistogramImpl *histogram_impl);
void DeleteHistogramImpl(HistogramImpl *histogram_impl);

#endif