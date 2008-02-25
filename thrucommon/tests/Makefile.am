INCLUDES = -I$(top_builddir)/src
LDADDS = ../src/libthrucommon.la

TESTS = SpreadTest

check_PROGRAMS=$(TESTS)

SpreadTest_SOURCES = SpreadTest.cpp
SpreadTest_LDADD = $(LDADDS)
SpreadTest_CXXFLAGS =  $(CPPUNIT_CFLAGS) -I../src
SpreadTest_LDFLAGS = $(CPPUNIT_LIBS) $(SPEAD_LIBS) $(SSL_LIBS) $(THRIFT_LIBS) $(UUID_LIBS) $(MEMCACHED_LIBS)
