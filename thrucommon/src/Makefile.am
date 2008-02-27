
pkgconfigdir = $(libdir)/pkgconfig

bin_PROGRAMS = thrurecorder
lib_LTLIBRARIES = libthrucommon.la
pkgconfig_DATA = thrucommon.pc

include_thrucommondir = $(includedir)/thrucommon
include_thrucommon_HEADERS = \
			     gen-cpp/Redo.h		\
			     gen-cpp/Redo_types.h	\
			     gen-cpp/EventLog.h		\
			     gen-cpp/EventLog_types.h	\
			     CircuitBreaker.h		\
			     ConfigFile.h		\
			     Hashing.h			\
			     memcache++.h		\
			     MemcacheHandle.h		\
			     ThruFileTransport.h	\
			     ReplicationRecorder.h	\
			     Spread.h			\
			     bloom_filter.hpp		\
			     utils.h

libthrucommon_la_SOURCES = \
			   gen-cpp/Redo.cpp			\
			   gen-cpp/Redo_types.cpp		\
			   gen-cpp/Redo_constants.cpp		\
			   gen-cpp/EventLog.cpp			\
			   gen-cpp/EventLog_types.cpp		\
			   gen-cpp/EventLog_constants.cpp	\
			   CircuitBreaker.cpp			\
			   ConfigFile.cpp			\
			   Hashing.cpp				\
			   ReplicationRecorder.cpp		\
			   Spread.cpp				\
			   ThruFileTransport.cpp

libthrucommon_la_CPPFLAGS = -Wall -Igen-cpp $(MEMCACHED_CFLAGS) $(SPEAD_CFLAGS) $(SSL_CFLAGS) $(THRIFT_CFLAGS) $(UUID_CFLAGS) $(SPEAD_CFLAGS)
libthrucommon_la_LDFLAGS = -Wall $(SPEAD_LIBS) $(SSL_LIBS) $(THRIFT_LIBS) $(UUID_LIBS) $(MEMCACHED_LIBS) $(SPEAD_LIBS)

LDADD = $(top_builddir)/src/libthrucommon.la

thrurecorder_CPPFLAGS = -Wall -Igen-cpp $(SPEAD_CFLAGS) $(THRIFT_CFLAGS)
thrurecorder_LDADD = $(LDADD)
thrurecorder_LDFLAGS = -Wall -rpath $(pkglibdir) $(SPEAD_LIBS) $(THRIFTNB_LIBS)
thrurecorder_SOURCES = \
		       thrurecorder.cpp

gen-cpp/Redo.cpp: Redo.thrift
	${THRIFT} -cpp $<

gen-cpp/EventLog.cpp: EventLog.thrift
	${THRIFT} -cpp $<

clean-local:
	rm -fr gen-cpp
