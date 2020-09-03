
CC=g++
CXXFLAGS= -std=c++11 -g -Wall -I./ 
LDFLAGS= -lpthread 

DSWARE_BASEDIR := ../../..
BUILD_DIR := $(shell pwd)

LIB_SOURCES= \
		core/core_workload.cc  \
		db/db_factory.cc   \
		db/hashtable_db.cc  \
		db/ycsb.cc \
		db/hwdb_db.cc \
		ycsb_c.cc \

m_src = $(LIB_SOURCES) $(HWDB_SOURCES)
m_lib = libycsb.a

src=$(BUILD_DIR)
dst=$(BUILD_DIR)

m_objs = $(subst .c,.o,$(m_src))
dst-objs = $(addprefix $(dst)/, $(m_objs))

dst-obj = $(dst)/$(addsuffix .o, $(basename $(m_lib)))
dst-lib = $(dst)/$(m_lib)

all: $(dst-lib)

.PHONY: FORCE

$(dst)/%.o: $(src)/%.c FORCE
	@echo obj is $@
	@echo src is @<
	$(CC) -c $(CXXFLAGS) $(LDFLAGS) $< -o $@

$(dst-lib): $(dst-objs)
	$(LD) -r $(dst-objs) -o $(dst-obj)
	$(AR) crus $(dst-lib) $(dst-obj)
	$(CP) -f $(dst-lib) $(DSWARE_BASEDIR)/build/lib

clean:
	$(RM) -f *.a
	$(RM) -f *.o
	$(RM) -f */*.o


