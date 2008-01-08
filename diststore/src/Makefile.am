bin_PROGRAMS = diststore
noinst_PROGRAMS = diststore_bench

diststore_SOURCES = \
		  gen-cpp/DistStore.cpp			\
		  gen-cpp/DistStore_types.cpp		\
		  gen-cpp/DistStore_constants.cpp	\
		  ConfigFile.cpp			\
		  DistStoreHandler.cpp			\
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

diststore_CXXFLAGS = -Wall -Igen-cpp
diststore_LDFLAGS = -Wall 

diststore_bench_SOURCES = \
			gen-cpp/DistStore.cpp		\
			gen-cpp/DistStore_types.cpp	\
			gen-cpp/DistStore_constants.cpp	\
			bench.cpp

diststore_bench_CXXFLAGS = -Wall -Igen-cpp
diststore_bench_LDFLAGS = -Wall

gen-cpp/DistStore.cpp: DistStore.thrift
	${THRIFT} -cpp DistStore.thrift

clean-local:
	rm -fr gen-cpp

