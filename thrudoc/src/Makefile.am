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

thrudoc_CXXFLAGS = -Wall $(CFLAGS) -Igen-cpp
thrudoc_LDFLAGS = -Wall $(CFLAGS)

thrudoc_bench_SOURCES = \
			gen-cpp/Thrudoc.cpp		\
			gen-cpp/Thrudoc_types.cpp	\
			gen-cpp/Thrudoc_constants.cpp	\
			bench.cpp

thrudoc_bench_CXXFLAGS = -Wall $(CFLAGS) -Igen-cpp
thrudoc_bench_LDFLAGS = -Wall $(CFLAGS) 

gen-cpp/Thrudoc.cpp: Thrudoc.thrift
	${THRIFT} -cpp Thrudoc.thrift

clean-local:
	rm -fr gen-cpp

