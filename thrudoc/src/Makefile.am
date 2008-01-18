bin_PROGRAMS = thrudoc
noinst_PROGRAMS = thrudoc_bench

thrudoc_SOURCES = \
		  gen-cpp/Thrudoc.cpp			\
		  gen-cpp/Thrudoc_types.cpp		\
		  gen-cpp/Thrudoc_constants.cpp		\
		  ConfigFile.cpp			\
		  ThrudocBackend.cpp			\
		  ThrudocHandler.cpp			\
		  BDBBackend.cpp			\
		  DiskBackend.cpp			\
		  MySQLBackend.cpp			\
		  MemcachedBackend.cpp			\
		  NBackend.cpp				\
		  S3Backend.cpp				\
		  SpreadBackend.cpp			\
		  base64.cpp				\
		  s3_glue.cpp				\
		  mysql_glue.cpp			\
		  main.cpp

thrudoc_CPPFLAGS = -Wall -Igen-cpp $(CURL_CFLAGS) $(MEMCACHED_CFLAGS) $(THRIFTNB_CFLAGS) $(SSL_CFLAGS) $(UUID_CFLAGS)
thrudoc_LDFLAGS = -Wall $(CURL_LIBS) $(THRIFTNB_LIBS) $(SSL_LIBS) $(UUID_LIBS) -levent -lmemcached

thrudoc_bench_SOURCES = \
			gen-cpp/Thrudoc.cpp		\
			gen-cpp/Thrudoc_types.cpp	\
			gen-cpp/Thrudoc_constants.cpp	\
			bench.cpp

thrudoc_bench_CPPFLAGS = -Wall -Igen-cpp $(THRIFT_CFLAGS)
thrudoc_bench_LDFLAGS = -Wall $(THRIFT_LDFLAGS) $(THRIFT_LIBS)

gen-cpp/Thrudoc.cpp: Thrudoc.thrift
	${THRIFT} -cpp $<

clean-local:
	rm -fr gen-cpp
