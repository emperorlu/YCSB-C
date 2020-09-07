CC = g++
# LDFLAGS = -O0 -pthread -Wno-pointer-to-int-cast -g -msse4.2 -fno-builtin -std=gnu99 -D_GNU_SOURCE -D__DPAX_LINUX_USR__
CXXFLAGS= -std=c++11 -g -Wall -I./ 
LDFLAGS= -lpthread -D__DPAX_LINUX_USR__
DEFS = 
CFLAGS = -I./include -I../include -I../../core/ -I../../../include/infrastructure/lwt/ -I../../../include \
		-I../../../include/infrastructure/diagnose/ -I../../../include/infrastructure/tracepoint/ \
		-I../../../include/infrastructure/umm/ -I../../../include/infrastructure/product_adapter/unifystorage/ \
		-I../../../include/infrastructure/upf -I../../../include/framework/ -I../../../include/framwork/ftds -I../../../include/drv/ -I../../../include/ctrl -I../../../include/infrastructure/tf/ \
		-I../../../include/infrastructure/osax/ -I../../../include/util -I../../../include/infrastructure/log/ \
		-I../../../../../third_party_groupware/HUAWEI_OFA_LINUX/common/include/ \


CFLAGS += $(DEFS)
AR = ar
ARFLAGS = rs
LIB_SOURCES =  \
		core/core_workload.cc  \
		db/db_factory.cc   \
		db/hashtable_db.cc  \
		db/ycsb.cc \
		db/hwdb_db.cc \
		lib/lwt_thread.cc \
		ycsb_c.cc \

LIBOBJECTS = \
		core_workload.o  \
		db_factory.o   \
		hashtable_db.o  \
		ycsb.o \
		hwdb_db.o \
		lwt_thread.o \
		ycsb_c.o \

TARGET_OBJS = $(LIB_SOURCES:.cc=)

LIBRARY = libycsb.a

.PHONY: clean
default:  all

all:  clean $(LIBRARY) 

clean: 
	rm -f $(LIBRARY)
	rm -f $(LIBOBJECTS)

$(LIBOBJECTS): 
	for sou_file in $(TARGET_OBJS) ; do \
	$(CC) $(CXXFLAGS) $(CFLAGS) $(LDFLAGS) -c $$sou_file.cc; \
	done;

$(LIBRARY): $(LIBOBJECTS)
	rm -f $@
	$(AR) $(ARFLAGS) $@ $^
	cp $@ ../../../build/lib
