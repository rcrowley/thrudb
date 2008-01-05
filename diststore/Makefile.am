bin_PROGRAMS = diststore
noinst_PROGRAMS = diststore_bench

diststore_SOURCES = \
		  gen-cpp/DistStore.cpp			\
		  gen-cpp/DistStore_types.cpp		\
		  gen-cpp/DistStore_constants.cpp	\
		  src/ConfigFile.cpp			\
		  src/DistStoreHandler.cpp		\
		  src/BDBBackend.cpp			\
		  src/DiskBackend.cpp			\
		  src/MySQLBackend.cpp			\
		  src/MemcachedBackend.cpp		\
		  src/S3Backend.cpp			\
		  src/SpreadBackend.cpp			\
		  src/base64.cpp			\
		  src/s3_glue.cpp			\
		  src/mysql_glue.cpp			\
		  src/main.cpp

diststore_CXXFLAGS = -Wall -I$(SANDBOX)/include -I$(SANDBOX)/include/thrift -I/usr/local/include/thrift -I/usr/include/thrift -I/usr/include/mysql -Igen-cpp 
diststore_LDFLAGS = -Wall -L$(SANDBOX)/lib -L/usr/local/lib -lthrift -lthriftnb -levent -llog4cxx -lmysqlclient_r -lmemcached -lboost_filesystem -lspread -lexpat -lcurl -ldb_cxx

diststore_bench_SOURCES = \
			gen-cpp/DistStore.cpp		\
			gen-cpp/DistStore_types.cpp	\
			gen-cpp/DistStore_constants.cpp	\
			src/bench.cpp

diststore_bench_CXXFLAGS = -Wall -I$(SANDBOX)/include -I$(SANDBOX)/include/thrift -I/usr/local/include/thrift -I/usr/include/thrift -Isrc -Igen-cpp -Isrc/common -Isrc/diststore
diststore_bench_LDFLAGS = -Wall -L$(SANDBOX)/lib -L/usr/local/lib -luuid -lthrift -levent -ltspread-core -llog4cxx

gen-cpp/DistStore.cpp: DistStore.thrift
	${THRIFT} -cpp DistStore.thrift

# test section taken from a Makefile.PL run...
FULLPERL      = /usr/bin/perl
FULLPERLRUN   = $(FULLPERL)
TEST_VERBOSE=0
TEST_TYPE=test_$(LINKTYPE)
TEST_FILE = test.pl
TEST_FILES = t/*.t
TESTDB_SW = -d

test :
	PERL_DL_NONLAZY=1 $(FULLPERLRUN) "-MExtUtils::Command::MM" "-e" "test_harness($(TEST_VERBOSE), '$(INST_LIB)', '$(INST_ARCHLIB)')" $(TEST_FILES)

clean-local:
	rm -fr gen-cpp

#install-exec-hook:
#	cp -r $(top_srcdir)/lib ${libdir}
#	cp -r $(top_srcdir)/scripts ${prefix}

