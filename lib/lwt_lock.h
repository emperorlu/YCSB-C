#ifndef _YCSB_LWT_LOCK_H_
#define _YCSB_LWT_LOCK_H_

#include "lwt_thread.h"

#define lock_t LwtMutex_t
#define LockInit(a)	  LwtMutex_Init(a)
#define Lock(a)	      LwtMutex_Lock(a)
#define Unlock(a)     LwtMutex_UnLock(a)
#define LockFree(a)	  LwtMutex_Free(a)

/* condition vars */
#define cond_t LwtCond_t
#define CondInit(c, mu)	    LwtCond_Init(c, mu)
#define CondFree(c) 	    LwtCond_Free(c)
#define Wait(c, l)   	    LwtCond_Wait(c)
#define Signal(c)	        LwtCond_Signal(c)
#define SignalAll(c)	    LwtCond_SignalAll(c)

#endif