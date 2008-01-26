pkgconfigdir = $(libdir)/pkgconfig

bin_PROGRAMS = thrudoc
noinst_PROGRAMS = thrudoc_bench
lib_LTLIBRARIES = libthrudoc.la
pkgconfig_DATA = thrudoc.pc

include_thrudocdir = $(includedir)/thrudoc
include_thrudoc_HEADERS = \
			     BDBBackend.h	\
			     BloomBackend.h	\
			     DiskBackend.h	\
			     LogBackend.h	\
			     MemcachedBackend.h	\
			     MySQLBackend.h	\
			     NBackend.h		\
			     NullBackend.h	\
			     S3Backend.h	\
			     SpreadBackend.h	\
			     StatsBackend.h	\
			     ThrudocBackend.h

libthrudoc_la_SOURCES = \
		  gen-cpp/Thrudoc.cpp			\
		  gen-cpp/Thrudoc_types.cpp		\
		  gen-cpp/Thrudoc_constants.cpp		\
		  ThrudocBackend.cpp			\
		  ThrudocHandler.cpp			\
		  LogBackend.cpp			\
		  BloomBackend.cpp			\
		  BDBBackend.cpp			\
		  DiskBackend.cpp			\
		  MySQLBackend.cpp			\
		  MemcachedBackend.cpp			\
		  NBackend.cpp				\
		  NullBackend.cpp			\
		  S3Backend.cpp				\
		  StatsBackend.cpp			\
		  SpreadBackend.cpp			\
		  base64.cpp				\
		  s3_glue.cpp				\
		  mysql_glue.cpp

libthrudoc_la_CPPFLAGS = -Wall -Igen-cpp $(CURL_CFLAGS) $(MEMCACHED_CFLAGS) $(THRIFTNB_CFLAGS) $(THRUCOMMON_CFLAGS) $(SSL_CFLAGS) $(UUID_CFLAGS)
libthrudoc_la_LDFLAGS = -Wall $(CURL_LIBS) $(THRIFTNB_LIBS) $(THRUCOMMON_LIBS) $(SSL_LIBS) $(UUID_LIBS) -levent -lmemcached

thrudoc_SOURCES = \
		  thrudoc.cpp

thrudoc_CPPFLAGS = -Wall -Igen-cpp $(CURL_CFLAGS) $(MEMCACHED_CFLAGS) $(THRIFTNB_CFLAGS) $(THRUCOMMON_CFLAGS) $(SSL_CFLAGS) $(UUID_CFLAGS)
thrudoc_LDFLAGS = -Wall $(CURL_LIBS) $(THRIFTNB_LIBS) $(THRUCOMMON_LIBS) $(SSL_LIBS) $(UUID_LIBS) -levent -lmemcached -lthrudoc

thrudoc_bench_SOURCES = \
			thrudoc_bench.cpp

thrudoc_bench_CPPFLAGS = -Wall -Igen-cpp $(THRIFT_CFLAGS)
thrudoc_bench_LDFLAGS = -Wall $(THRIFT_LDFLAGS) $(THRIFT_LIBS) -lthrudoc

gen-cpp/Thrudoc.cpp: Thrudoc.thrift
	${THRIFT} -cpp $<

clean-local:
	rm -fr gen-cpp
