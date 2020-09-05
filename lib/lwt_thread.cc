#include "lwt_thread.h"

// LWT Mutex
void LwtMutex_Init(LwtMutex_t *mu) {
    mu->lock_ = malloc(sizeof(dpax_spinlock_t));
    atomic_init(&mu->flag, 0);
    dpax_spinlock_init((dpax_spinlock_t*)(mu->lock_));
}

void LwtMutex_Free(LwtMutex_t *mu) {
    free(mu->lock_);
}

void LwtMutex_Lock(LwtMutex_t *mu) {
    // dpax_spinlock_lock((dpax_spinlock_t*)(mu->lock_));
    while(1) {
        dpax_spinlock_lock((dpax_spinlock_t*)(mu->lock_));
        int flag = atomic_load(&mu->flag);
        if(flag == 0) {
            atomic_store(&mu->flag, 1);
            dpax_spinlock_unlock((dpax_spinlock_t*)(mu->lock_));
            break;
        }
        dpax_spinlock_unlock((dpax_spinlock_t*)(mu->lock_));
        DPLWT_YIELD();
    }
}

void LwtMutex_UnLock(LwtMutex_t *mu) {
    // dpax_spinlock_unlock((dpax_spinlock_t*)(mu->lock_));
    atomic_store(&mu->flag, 0);
}

// LWT Cond
void LwtCond_Init(LwtCond_t *cond, LwtMutex_t *mu) {
    cond->mu_ = mu;
    cond->waiters_ = 0;
    cond->down_ = malloc(sizeof(dplwt_sem_s));
    LwtCall("LwtCond down_ init", DPLWT_SEM_INIT((dplwt_sem_s*)(cond->down_)));
}

void LwtCond_Free(LwtCond_t *cond) {
    LwtCall("LwtCond down_ destory", DPLWT_SEM_DESTROY((dplwt_sem_s*)(cond->down_)));
}

void LwtCond_Wait(LwtCond_t *cond) {
    cond->waiters_++;
    LwtMutex_UnLock(cond->mu_);
    LwtCall("LwtCond_Wait down down_", DPLWT_SEM_DOWN((dplwt_sem_s*)(cond->down_)));
    LwtMutex_Lock(cond->mu_);
}

void LwtCond_Signal(LwtCond_t *cond) {
    if (cond->waiters_ > 0){
        cond->waiters_--;
        LwtCall("LwtCond_Signal up down_", DPLWT_SEM_UP((dplwt_sem_s*)(cond->down_)));
    }
}

void LwtCond_SignalAll(LwtCond_t *cond) {
    while(cond->waiters_ > 0) {
        cond->waiters_--;
        LwtCall("LwtCond_Signal up down_", DPLWT_SEM_UP((dplwt_sem_s*)(cond->down_)));
    }
}