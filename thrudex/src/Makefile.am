pkgconfigdir = $(libdir)/pkgconfig

bin_PROGRAMS = thrudex thrudex_bench
noinst_PROGRAMS = thrudex_bench
lib_LTLIBRARIES = libthrudex.la
pkgconfig_DATA = thrudex.pc


include_thrudexdir = $(includedir)/thrudex

libthrudex_la_SOURCES = \
		  gen-cpp/Thrudex.cpp			\
		  gen-cpp/Thrudex_types.cpp		\
		  gen-cpp/Thrudex_constants.cpp		\
		  ThrudexHandler.cpp			\
		  ThrudexBackend.cpp			\
		  CLuceneBackend.cpp			\
		  CLuceneRAMDirectory.cpp               \
		  CLuceneIndex.cpp			\
		  StatsBackend.cpp			\
		  UpdateFilter.cpp

thrudex_SOURCES = \
		  main.cpp


AM_CPPFLAGS = -Wall -Igen-cpp $(MEMCACHED_CFLAGS) $(CURL_CFLAGS) $(THRIFTNB_CFLAGS) $(THRUCOMMON_CFLAGS) $(SSL_CFLAGS) $(UUID_CFLAGS)
AM_LDFLAGS = -Wall $(CURL_LIBS) $(THRIFTNB_LIBS) $(THRUCOMMON_LIBS) $(SSL_LIBS) $(UUID_LIBS) -levent -lclucene -lboost_filesystem

LDADD = $(top_builddir)/src/libthrudex.la

thrudex_bench_SOURCES = \
			thrudex_bench.cpp

thrudex_bench_CPPFLAGS = -Wall -Igen-cpp $(THRIFTNB_CFLAGS) $(THRUCOMMON_CFLAGS)
thrudex_bench_LDFLAGS = -Wall $(THRIFTNB_LDFLAGS) $(THRIFTNB_LIBS) -lthrudex

gen-cpp/Thrudex.cpp: Thrudex.thrift
	${THRIFT} -cpp $<

clean-local:
	rm -fr gen-cpp
