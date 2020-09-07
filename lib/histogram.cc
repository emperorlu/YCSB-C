//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.


#include "histogram.h"

#include <inttypes.h>
#include <assert.h>
#include <math.h>
#include <stdio.h>
#include <stdlib.h>

double my_sqrt(double x);

int CreateHistogramBucketMapper(HistogramBucketMapper* mapper) {
    HistogramBucketMapper *temp = mapper;
    temp->size_ = 0;
    temp->bucketValues_[temp->size_++] = 1;
    temp->bucketValues_[temp->size_++] = 2;

    // uint64_t start_nums[31] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18, 20, 25, 30, 35, 40, 45,
    //     50, 60, 70, 80, 90, 100, 120, 140, 160, 180, 200};
    // for(uint64_t i = 0; i < 31; i++){
    //     temp->bucketValues_[temp->size_++] = start_nums[i];
    // } //小时延密度放大一些。


    double bucket_val = temp->bucketValues_[temp->size_ - 1];
    while (temp->size_ < MAX_MAP_SIZE && (bucket_val = 1.5 * bucket_val) <= (double)max_uint64) {
        uint64_t tmp_val = (uint64_t)bucket_val;
        uint64_t pow_of_ten = 1;
        while (tmp_val / 10 > 10) {
            tmp_val /= 10;
            pow_of_ten *= 10;
        }
        tmp_val *= pow_of_ten;
        temp->bucketValues_[temp->size_++] = tmp_val;
    }
    temp->maxBucketValue_ = temp->bucketValues_[temp->size_-1];
    temp->minBucketValue_ = temp->bucketValues_[0];

    return 0;
}

void DeleteHistogramBucketMapper(HistogramBucketMapper* mapper){
    mapper = NULL;
}

//二分查找大于等于的第一个下标,不存在情况没有讨论，外面限制了范围
uint32_t GetLowerIndex(uint64_t *values, uint64_t size, const uint64_t k) {
    uint32_t l = 0, r = size - 1;
    while(l < r) {
        uint32_t mid = (l + r) / 2;
        uint64_t midK = values[mid];

        if(midK < k) {
            l = mid + 1;
        } else {
            r = mid;
        }
    }
    return l;
}

//HistogramBucketMapper::
size_t IndexForValue(HistogramBucketMapper *bucketMapper, const uint64_t value) {
    if(value < bucketMapper->maxBucketValue_ && value > bucketMapper->minBucketValue_){
        return GetLowerIndex(bucketMapper->bucketValues_, bucketMapper->size_, value);
    }
    else if(value <= bucketMapper->minBucketValue_){
        return 0;
    }
    else{
        return bucketMapper->size_ - 1;
    }
}

void HistogramStatClear(HistogramStat *stat) {
    atomic_init(&stat->min_, 0);
    atomic_init(&stat->max_, 0);
    atomic_init(&stat->num_, 0);
    atomic_init(&stat->sum_, 0);
    atomic_init(&stat->sum_squares_, 0);
    for(int i = 0; i < stat->num_buckets_; i++){
        atomic_init(&(stat->buckets_[i]), 0);
    }
}

int CreateHistogramStat(HistogramStat* stat) {
    HistogramStat *temp = stat;
    int ret = CreateHistogramBucketMapper(&temp->bucketMapper_);
    if(ret != 0) return -1;
    temp->num_buckets_ = temp->bucketMapper_.size_;
    assert(temp->num_buckets_ <= sizeof(temp->buckets_) / sizeof(*temp->buckets_));
    // assert(temp->num_buckets_ == sizeof(temp->buckets_) / sizeof(*temp->buckets_));
    HistogramStatClear(temp);
    return 0;
}

void DeleteHistogramStat(HistogramStat* stat){
    DeleteHistogramBucketMapper(&(stat->bucketMapper_));
    stat = NULL;
}

//
int HistogramStatEmpty(HistogramStat *stat) { 
    uint64_t num = atomic_load_explicit(&stat->num_, memory_order_relaxed);
    return num == 0; 
}

void HistogramStatAdd(HistogramStat *stat, uint64_t value) {
    // This function is designed to be lock free, as it's in the critical path
    // of any operation. Each individual value is atomic and the order of updates
    // by concurrent threads is tolerable.
    size_t index = IndexForValue(&stat->bucketMapper_, value);
    assert(index < stat->num_buckets_);
    atomic_fetch_add_explicit(&(stat->buckets_[index]), 1, memory_order_relaxed);

    uint64_t old_min = atomic_load_explicit(&stat->min_, memory_order_relaxed);
    if (value < old_min) {
        atomic_exchange_explicit(&stat->min_, value, memory_order_relaxed);
    }

    uint64_t old_max = atomic_load_explicit(&stat->max_, memory_order_relaxed);
    if (value > old_max) {
        atomic_exchange_explicit(&stat->max_, value, memory_order_relaxed);
    }

    atomic_fetch_add_explicit(&(stat->num_), 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&(stat->sum_), value, memory_order_relaxed);
    atomic_fetch_add_explicit(&(stat->sum_squares_), value * value, memory_order_relaxed);
}

void HistogramStatMerge(HistogramStat *stat, HistogramStat *other) {
    // This function needs to be performned with the outer lock acquired
    // However, atomic operation on every member is still need, since Add()
    // requires no lock and value update can still happen concurrently
    uint64_t old_min = atomic_load_explicit(&stat->min_, memory_order_relaxed);
    uint64_t other_min = atomic_load_explicit(&other->min_, memory_order_relaxed);
    // while (other_min < old_min &&
    //         atomic_compare_exchange_weak(&stat->min_, &old_min, other_min)) {}
    if(other_min < old_min) atomic_exchange_explicit(&stat->min_, other_min, memory_order_relaxed);

    uint64_t old_max = atomic_load_explicit(&stat->max_, memory_order_relaxed);
    uint64_t other_max = atomic_load_explicit(&other->max_, memory_order_relaxed);
    // while (other_max > old_max &&
    //         atomic_compare_exchange_weak(&stat->max_, &old_max, other_max)) {}
    if(other_max > old_max) atomic_exchange_explicit(&stat->max_, other_max, memory_order_relaxed);

    uint64_t other_num = atomic_load_explicit(&other->num_, memory_order_relaxed);
    atomic_fetch_add_explicit(&(stat->num_), other_num, memory_order_relaxed);

    uint64_t other_sum = atomic_load_explicit(&other->sum_, memory_order_relaxed);
    atomic_fetch_add_explicit(&(stat->sum_), other_sum, memory_order_relaxed);
    
    uint64_t other_sum_squares = atomic_load_explicit(&other->sum_squares_, memory_order_relaxed);
    atomic_fetch_add_explicit(&(stat->sum_squares_), other_sum_squares, memory_order_relaxed);

    for (unsigned int b = 0; b < stat->num_buckets_; b++) {
        uint64_t other_bucket = atomic_load_explicit(&(other->buckets_[b]), memory_order_relaxed);
        atomic_fetch_add_explicit(&(stat->buckets_[b]), other_bucket, memory_order_relaxed);
    }
}

double HistogramStatPercentile(HistogramStat *stat, double p) {
    uint64_t num = atomic_load_explicit(&stat->num_, memory_order_relaxed);
    double threshold = num * (p / 100.0);
    uint64_t cumulative_sum = 0;
    for (unsigned int b = 0; b < stat->num_buckets_; b++) {
        uint64_t bucket_value = atomic_load_explicit(&(stat->buckets_[b]), memory_order_relaxed);
        cumulative_sum += bucket_value;
        if (cumulative_sum >= threshold) {
            // Scale linearly within this bucket
            uint64_t left_point = (b == 0) ? 0 : stat->bucketMapper_.bucketValues_[b-1]; //BucketLimit(b-1);
            uint64_t right_point = stat->bucketMapper_.bucketValues_[b];
            uint64_t left_sum = cumulative_sum - bucket_value;
            uint64_t right_sum = cumulative_sum;
            double pos = 0;
            uint64_t right_left_diff = right_sum - left_sum;
            if (right_left_diff != 0) {
                pos = (threshold - left_sum) / right_left_diff;
            }
            double r = left_point + (right_point - left_point) * pos;
            uint64_t cur_min = atomic_load_explicit(&stat->min_, memory_order_relaxed);
            uint64_t cur_max = atomic_load_explicit(&stat->max_, memory_order_relaxed);
            if (r < cur_min) r = (double)cur_min;
            if (r > cur_max) r = (double)cur_max;
            return r;
        }
    }
    double max = (double)atomic_load_explicit(&stat->max_, memory_order_relaxed);
    return max;
}

double HistogramStatMedian(HistogramStat *stat) {
  return HistogramStatPercentile(stat, 50.0);
}

double HistogramStatAverage(HistogramStat *stat) {
    uint64_t cur_num = atomic_load_explicit(&stat->num_, memory_order_relaxed);
    uint64_t cur_sum = atomic_load_explicit(&stat->sum_, memory_order_relaxed);
    if (cur_num == 0) return 0;
    return (double)(cur_sum) / (double)(cur_num);
}
//牛顿迭代法求平方根
double my_sqrt(double x) {
    if (x == 0) {
    	return 0;
    }
    double last = 0.0;
    double res = 1.0;
    while (res != last) {
        last = res;
        res = (res + x / res) / 2;
    }
    return res;
}

double HistogramStatStandardDeviation(HistogramStat *stat) {
    uint64_t cur_num = atomic_load_explicit(&stat->num_, memory_order_relaxed);
    uint64_t cur_sum = atomic_load_explicit(&stat->sum_, memory_order_relaxed);
    uint64_t cur_sum_squares = atomic_load_explicit(&stat->sum_squares_, memory_order_relaxed);
    if (cur_num == 0) return 0;
    double variance =
        (double)(cur_sum_squares * cur_num - cur_sum * cur_sum) /
        (double)(cur_num * cur_num);
    //return sqrt(variance);   //sqrt可能找不到依赖库
    return my_sqrt(variance);
}

char* HistogramStatToString(HistogramStat *stat) {
    uint64_t cur_num = atomic_load_explicit(&stat->num_, memory_order_relaxed);

    char * msg = (char*)malloc(10000 * sizeof(char));
    memset(msg, 0, 10000);
    char buf[4096];

    snprintf(buf, sizeof(buf),
            "Count: %" PRIu64 " Average: %.4f  StdDev: %.2f\n",
            cur_num, HistogramStatAverage(stat), HistogramStatStandardDeviation(stat));
    strcat(msg, buf);

    uint64_t min = atomic_load_explicit(&stat->min_, memory_order_relaxed);
    uint64_t max = atomic_load_explicit(&stat->max_, memory_order_relaxed);
    snprintf(buf, sizeof(buf),
            "Min: %" PRIu64 "  Median: %.4f  Max: %" PRIu64 "\n",
            (cur_num == 0 ? 0 : min), HistogramStatMedian(stat), (cur_num == 0 ? 0 : max));
    strcat(msg, buf);

    snprintf(buf, sizeof(buf),
            "Percentiles: "
            "P50: %.2f P90: %.2f P99: %.2f P99.9: %.2f P99.99: %.2f\n",
            HistogramStatPercentile(stat, 50), 
            HistogramStatPercentile(stat, 90), 
            HistogramStatPercentile(stat, 99), 
            HistogramStatPercentile(stat, 99.9),
            HistogramStatPercentile(stat, 99.99));
    strcat(msg, buf);
    
    strcpy(buf, "------------------------------------------------------\n");
    strcat(msg, buf);
    
    if (cur_num == 0) return msg;   // all buckets are empty
    const double mult = 100.0 / cur_num;
    uint64_t cumulative_sum = 0;
    for (unsigned int b = 0; b < stat->num_buckets_; b++) {
        uint64_t bucket_value = atomic_load_explicit(&(stat->buckets_[b]), memory_order_relaxed);
        if (bucket_value <= 0.0) continue;
        cumulative_sum += bucket_value;
        snprintf(buf, sizeof(buf),
                "%c %7" PRIu64 ", %7" PRIu64 " ] %8" PRIu64 " %7.3f%% %7.3f%% ",
                (b == 0) ? '[' : '(',
                (b == 0) ? 0 : stat->bucketMapper_.bucketValues_[b-1],  // left
                stat->bucketMapper_.bucketValues_[b],  // right
                bucket_value,                   // count
                (mult * bucket_value),           // percentage
                (mult * cumulative_sum));       // cumulative percentage
        strcat(msg, buf);

        // Add hash marks based on percentage; 20 marks for 100%.
        size_t marks = (size_t)(mult * bucket_value / 5 + 0.5);

        for(int i = 0 ; i < marks; i++){
            strcat(msg, "#");
        }
        strcat(msg, "\n");
    }
    return msg;
}

void HistogramImplClear(OBJ impl) {
    if(impl == NULL) return;
    HistogramImpl *histogram_impl = (HistogramImpl*)impl;
    // Lock(&histogram_impl->lock_);
    HistogramStatClear(&histogram_impl->stats_);
    // Unlock(&histogram_impl->lock_);
}

int HistogramImplEmpty(OBJ impl) {
    if(impl == NULL) return 0;
    HistogramImpl *histogram_impl = (HistogramImpl*)impl;
    return HistogramStatEmpty(&histogram_impl->stats_);
}

void HistogramImplAdd(OBJ impl, uint64_t value) {
    if(impl == NULL) return;
    HistogramImpl *histogram_impl = (HistogramImpl*)impl;
    HistogramStatAdd(&histogram_impl->stats_, value);
}

void HistogramImplMerge(OBJ impl, OBJ other) {
    if(impl == NULL || other == NULL) return;
    HistogramImpl *histogram_impl = (HistogramImpl*)impl;
    HistogramImpl *histogram_other = (HistogramImpl*)other;
    // Lock(&histogram_impl->lock_);
    HistogramStatMerge(&histogram_impl->stats_, &histogram_other->stats_);
    // Unlock(&histogram_impl->lock_);
}

double HistogramImplMedian(OBJ impl) {
    if(impl == NULL) return 0;
    HistogramImpl *histogram_impl = (HistogramImpl*)impl;
    return HistogramStatMedian(&histogram_impl->stats_);
}

double HistogramImplPercentile(OBJ impl, double p) {
    if(impl == NULL) return 0;
    HistogramImpl *histogram_impl = (HistogramImpl*)impl;
    return HistogramStatPercentile(&histogram_impl->stats_, p);
}

double HistogramImplAverage(OBJ impl) {
    if(impl == NULL) return 0;
    HistogramImpl *histogram_impl = (HistogramImpl*)impl;
    return HistogramStatAverage(&histogram_impl->stats_);
}

double HistogramImplStandardDeviation(OBJ impl) {
    if(impl == NULL) return 0;
    HistogramImpl *histogram_impl = (HistogramImpl*)impl;
    return HistogramStatStandardDeviation(&histogram_impl->stats_);
}

char* HistogramImplToString(OBJ impl) {
    if(impl == NULL) return NULL;
    HistogramImpl *histogram_impl = (HistogramImpl*)impl;
    return HistogramStatToString(&histogram_impl->stats_);
}

int CreateHistogramImpl(HistogramImpl *histogram_impl) {
    HistogramImpl *obj = histogram_impl;
    if(obj->init_) {
        HistogramImplClear(obj);   
        return 0;
    }
    int ret = CreateHistogramStat(&obj->stats_);
    if (ret != 0) return -1;
    // LockInit(&obj->lock_);

    obj->interface.Clear = HistogramImplClear;
    obj->interface.Empty = HistogramImplEmpty;
    obj->interface.Add = HistogramImplAdd;
    obj->interface.Merge = HistogramImplMerge;
    obj->interface.ToString = HistogramImplToString;
    obj->interface.Median = HistogramImplMedian;
    obj->interface.Percentile = HistogramImplPercentile;
    obj->interface.Average = HistogramImplAverage;
    obj->interface.StandardDeviation = HistogramImplStandardDeviation;
    
    obj->init_ = true;

    return 0;
}

void DeleteHistogramImpl(HistogramImpl *histogram_impl){
    DeleteHistogramStat(&(histogram_impl->stats_));
    // LockFree(&(histogram_impl->lock_));
    histogram_impl = NULL;
}

