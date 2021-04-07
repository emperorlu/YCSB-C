
CC=g++
CXXFLAGS= -std=c++11 -g -O0 -I./ -w -rdynamic
LDFLAGS= -lpthread -lz

SPDK_DIR ?= ../spdk
SPDK_ROOT_DIR := $(abspath $(SPDK_DIR))
SPDK_LIB_DIR = $(SPDK_DIR)/build/lib
DPDK_LIB_DIR = $(SPDK_DIR)/dpdk/build-tmp

# CXXFLAGS += -I$(SPDK_DIR)/include

LIB_SOURCES= \
		core/core_workload.cc  \
		db/db_factory.cc   \
		db/hashtable_db.cc  \
		lib/histogram.cc \
		$(SPDK_DIR)/lib/wiredtiger/env_spdk.cc \

include $(SPDK_ROOT_DIR)/lib/wiredtiger/spdk.wiredtiger.mk

##SPDK
SPDK_LIBS =  -Wl,--whole-archive -Wl,$(SPDK_LIB_DIR)/libspdk_spdkwt.a -Wl,$(SPDK_LIB_DIR)/libspdk_event.a -Wl,$(SPDK_LIB_DIR)/libspdk_log.a \
	-Wl,$(SPDK_LIB_DIR)/libspdk_thread.a -Wl,$(SPDK_LIB_DIR)/libspdk_rpc.a -Wl,$(SPDK_LIB_DIR)/libspdk_blobfs.a \
	-Wl,$(SPDK_LIB_DIR)/libspdk_trace.a -Wl,$(SPDK_LIB_DIR)/libspdk_env_dpdk.a -Wl,$(SPDK_LIB_DIR)/libspdk_json.a \
	-Wl,$(SPDK_LIB_DIR)/libspdk_blob.a -Wl,$(SPDK_LIB_DIR)/libspdk_bdev.a -Wl,$(SPDK_LIB_DIR)/libspdk_jsonrpc.a \
	-Wl,$(SPDK_LIB_DIR)/libspdk_conf.a -Wl,$(SPDK_LIB_DIR)/libspdk_blob_bdev.a -Wl,$(SPDK_LIB_DIR)/libspdk_util.a \
	-Wl,$(SPDK_LIB_DIR)/libspdk_notify.a $(SPDK_DIR)/isa-l/.libs/libisal.a -Wl,--no-whole-archive\
	-L$(SPDK_DIR)/dpdk/build/lib -Wl,--whole-archive -Wl,-lrte_eal -Wl,-lrte_mempool -Wl,-lrte_bus_pci -Wl,-lrte_kvargs -Wl,-lrte_ring -Wl,-lrte_pci -Wl,-lrte_telemetry -Wl,-lrte_mempool_ring -Wl,--no-whole-archive \
	-Wl,--whole-archive $(DPDK_LIB_DIR)/drivers/librte_mempool_ring.a -Wl,--no-whole-archive -lrt -luuid
##

##HWDB
HWDB_SOURCES= db/hwdb_db.cc
HWDB_LIBRARY= -lhwdb
HWDB_DEFS= -DYCSB_HWDB
HWDB_OBJECTS=$(HWDB_SOURCES:.cc=.o)
##

##rocksdb
ROCKSDB_SOURCES= db/rocksdb_db.cc
ROCKSDB_LIBRARY= -lrocksdb -lz -lzstd -llz4 -lsnappy
ROCKSDB_DEFS= -DYCSB_ROCKSDB
ROCKSDB_OBJECTS=$(ROCKSDB_SOURCES:.cc=.o)
##

##rocksdb
WIREDTIGER_SOURCES= db/wiredtiger_db.cc
WIREDTIGER_LIBRARY= -lwiredtiger
WIREDTIGER_DEFS= -DYCSB_WIREDTIGER
WIREDTIGER_OBJECTS=$(WIREDTIGER_SOURCES:.cc=.o)
##

OBJECTS=$(LIB_SOURCES:.cc=.o)
EXEC=ycsbc

ONLY_HWDB_SOURCES=$(LIB_SOURCES) $(HWDB_SOURCES)
ONLY_ROCKSDB_SOURCES=$(LIB_SOURCES) $(ROCKSDB_SOURCES)
ONLY_WIREDTIGER_SOURCES=$(LIB_SOURCES) $(WIREDTIGER_SOURCES)
ALL_SOURCES=$(LIB_SOURCES) $(HWDB_SOURCES) $(ROCKSDB_SOURCES) $(WIREDTIGER_SOURCES)

all: clean
	$(CC) $(CXXFLAGS) $(HWDB_DEFS) $(ROCKSDB_DEFS) $(WIREDTIGER_DEFS) ycsbc.cc $(ALL_SOURCES) -o $(EXEC) $(LDFLAGS) $(HWDB_LIBRARY) $(ROCKSDB_LIBRARY) $(WIREDTIGER_LIBRARY)

hwdb: clean
	$(CC) $(CXXFLAGS) $(HWDB_DEFS) ycsbc.cc $(ONLY_HWDB_SOURCES) -o $(EXEC) $(LDFLAGS) $(HWDB_LIBRARY)

rocksdb: clean
	$(CC) $(CXXFLAGS) $(ROCKSDB_DEFS) ycsbc.cc $(ONLY_ROCKSDB_SOURCES) -o $(EXEC) $(LDFLAGS) $(ROCKSDB_LIBRARY)

wiredtiger: clean
	$(CC) $(CXXFLAGS) $(WIREDTIGER_DEFS) ycsbc.cc $(ONLY_WIREDTIGER_SOURCES) -o $(EXEC) $(LDFLAGS) $(WIREDTIGER_LIBRARY) $(AM_LINK)

clean:
	rm -f $(EXEC) $(OBJECTS) $(WIREDTIGER_OBJECTS)

.PHONY: clean rocksdb hwdb wiredtiger

