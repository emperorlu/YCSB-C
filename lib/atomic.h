#ifndef _HWDB_UTILS_ATOMIC_H_
#define _HWDB_UTILS_ATOMIC_H_

#define HW_PLATFORM
#ifdef HW_PLATFORM

#ifdef __cplusplus   
extern "C"{
#endif /* __cplusplus */
#include "/usr/include/vos/lvos_atomic.h"

#ifdef __cplusplus
}
#endif /* __cplusplus */

#define atomic_int_t   atomic_t
#define atomic_bool_t  atomic_t
#define atomic_uint32_t  atomic_t
#define atomic_uint64_t atomic_t

#define atomic_store_explicit(ptr, val, mo) atomic_set(ptr, val)
#define atomic_exchange_explicit(ptr, val, mo) atomic_set(ptr, val)

#define atomic_init(ptr, val) atomic_store_explicit(ptr, val, __ATOMIC_RELAXED)

#define atomic_store(ptr, val) atomic_store_explicit(ptr, val, __ATOMIC_SEQ_CST)

#define atomic_load_explicit(ptr, mo) atomic_read(ptr)

#define atomic_load(ptr) atomic_load_explicit(ptr, __ATOMIC_SEQ_CST)

#define atomic_fetch_add_explicit(ptr, val, mo) atomic_add_return(val, ptr)

#define atomic_fetch_add(ptr, val) atomic_fetch_add_explicit(ptr, val, __ATOMIC_SEQ_CST)

#define atomic_fetch_sub_explicit(ptr, val, mo) atomic_sub_return(val, ptr)

#define atomic_fetch_sub(ptr, val) atomic_fetch_sub_explicit(ptr, val, __ATOMIC_SEQ_CST)

#else
#include<stdatomic.h>

#define atomic_int_t   atomic_int
#define atomic_bool_t  atomic_bool
#define atomic_uint32_t  atomic_uint
#define atomic_uint64_t atomic_ullong

// #define atomic_init(ptr, val)      atomic_init(ptr, val)
// #define atomic_store(ptr, val)     atomic_store(ptr, val)
// #define atomic_load(ptr)           atomic_load(ptr)
// #define atomic_fetch_add(ptr, val) atomic_fetch_add(ptr, val)
// #define atomic_fetch_sub(ptr, val) atomic_fetch_sub(ptr, val)
// #define atomic_exchange(ptr, val)  atomic_exchange(ptr, val)

#endif









#endif