#ifndef YCSB_LWT_THREAD_H
#define YCSB_LWT_THREAD_H

#ifdef __cplusplus   
extern "C"{
#endif /* __cplusplus */

#include "atomic.h"
#include "dplwt_api.h"
#include "pool_pub.h"

#ifdef __cplusplus
}
#endif /* __cplusplus */

static void LwtCall(const char* label, int result) {
    if (result != 0) {
        fprintf(stderr, "LWT %s: %s\n", label, strerror(result));
        abort();
    }
}
/*
 * DPLWT_BYID_DISPATCH_SET 含义不明，可能是：
 * 内部调用 byId = 1
 * 外部调用，如db_bench， byId = 0
 */
static inline void thread_add_task(void (*function)(void* arg), void* arg, int byId, int extra) {
    dplwt_id_t lwt_id;
    dplwt_attr_s attr;
    DPLWT_ATTR_INIT(&attr);
    // attr.uiId = (GET_REQ_THREAD_ID() + extra) % 3 + 6;  // 测plog用
    attr.uiId = GET_REQ_THREAD_ID();
    attr.ucPri = DPLWT_PRI_WEIGHT_3;
    attr.pfnExtCbk = NULL;
    attr.pvExtArg = NULL;
    if (byId) {
        DPLWT_BYID_DISPATCH_SET(&attr);
    }
    int result = DPLWT_CREATE(&lwt_id, function, arg, &attr, REQ_STAT_TYPE_298);
}

/*
 *  LWT Mutex
 */
typedef struct LwtMutex {
    void* lock_;
    atomic_int_t flag;
} LwtMutex_t;

void LwtMutex_Init(LwtMutex_t *mu);
void LwtMutex_Free(LwtMutex_t *mu);
void LwtMutex_Lock(LwtMutex_t *mu);
void LwtMutex_UnLock(LwtMutex_t *mu);

/*
 *  LWT Cond
 */
typedef struct LwtCond {
    LwtMutex_t *mu_;
    int waiters_;
    void *down_;
    // void *up_;
} LwtCond_t;

void LwtCond_Init(LwtCond_t *cond, LwtMutex_t *mu);
void LwtCond_Free(LwtCond_t *cond);
void LwtCond_Wait(LwtCond_t *cond);
void LwtCond_Signal(LwtCond_t *cond);
void LwtCond_SignalAll(LwtCond_t *cond);

#endif