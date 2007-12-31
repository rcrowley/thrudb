bin_PROGRAMS = mytable

mytable_SOURCES = \
		  gen-cpp/MyTable.cpp		\
		  gen-cpp/MyTable_types.cpp	\
		  gen-cpp/MyTable_constants.cpp	\
		  src/ConfigFile.cpp		\
		  src/MyTableHandler.cpp	\
		  src/MySQLBackend.cpp		\
		  src/MemcachedBackend.cpp	\
		  src/mysql_glue.cpp		\
		  src/main.cpp

mytable_CXXFLAGS = -Wall -I$(SANDBOX)/include -I$(SANDBOX)/include/thrift -I/usr/local/include/thrift -I/usr/include/thrift -I/usr/include/mysql -Igen-cpp 

mytable_LDFLAGS = -Wall -L$(SANDBOX)/lib -L/usr/local/lib -lthrift -lthriftnb -levent -llog4cxx -lmysqlclient_r -lmemcached

gen-cpp/MyTable.cpp: MyTable.thrift
	${THRIFT} -cpp MyTable.thrift

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

