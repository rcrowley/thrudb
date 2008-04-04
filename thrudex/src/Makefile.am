pkgconfigdir = $(libdir)/pkgconfig

bin_PROGRAMS = thrudex thrudex_bench thrudex_replay
noinst_PROGRAMS = thrudex_bench
lib_LTLIBRARIES = libthrudex.la
pkgconfig_DATA = thrudex.pc


include_thrudexdir = $(includedir)/thrudex

libthrudex_la_SOURCES = \
		  gen-cpp/Thrudex.cpp			\
		  gen-cpp/Thrudex_types.cpp		\
		  gen-cpp/Thrudex_constants.cpp		\
		  app_helpers.cpp			\
		  ThrudexHandler.cpp			\
		  LogBackend.cpp			\
		  ThrudexBackend.cpp			\
		  CLuceneBackend.cpp			\
		  CLuceneRAMDirectory.cpp               \
		  CLuceneIndex.cpp			\
		  StatsBackend.cpp			\
		  UpdateFilter.cpp

thrudex_SOURCES = \
		  thrudex.cpp


AM_CPPFLAGS = -Wall -Igen-cpp $(MEMCACHED_CFLAGS) $(CURL_CFLAGS) $(THRIFTNB_CFLAGS) $(THRUCOMMON_CFLAGS) $(SSL_CFLAGS) $(UUID_CFLAGS)
AM_LDFLAGS = -Wall $(CURL_LIBS) $(THRIFTNB_LIBS) $(THRUCOMMON_LIBS) $(SSL_LIBS) $(UUID_LIBS) -levent -lclucene -lboost_filesystem

LDADD = $(top_builddir)/src/libthrudex.la

thrudex_replay_CPPFLAGS = -Wall -Igen-cpp $(CURL_CFLAGS) $(MEMCACHED_CFLAGS) $(THRIFTNB_CFLAGS) $(THRUCOMMON_CFLAGS) $(SSL_CFLAGS) $(UUID_CFLAGS)
thrudex_replay_LDADD = $(LDADD)
thrudex_replay_LDFLAGS = -Wall -rpath $(pkglibdir) $(CURL_LIBS) $(MEMCACHED_LIBS) $(THRIFTNB_LIBS) $(THRUCOMMON_LIBS) $(SSL_LIBS) $(UUID_LIBS) 
thrudex_replay_SOURCES = \
		  thrudex_replay.cpp

thrudex_bench_SOURCES = \
			thrudex_bench.cpp

thrudex_bench_CPPFLAGS = -Wall -Igen-cpp $(THRIFTNB_CFLAGS) $(THRUCOMMON_CFLAGS)
thrudex_bench_LDFLAGS = -Wall $(THRIFTNB_LDFLAGS) $(THRIFTNB_LIBS) -lthrudex

gen-cpp/Thrudex.cpp: Thrudex.thrift
	${THRIFT} -cpp $<

clean-local:
	rm -fr gen-cpp
