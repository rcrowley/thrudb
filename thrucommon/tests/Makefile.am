# TODO: make sure we link against our thrucommon and not an installed one

INCLUDES = -I$(top_builddir)/src
LDADDS = $(top_builddir)/src/libthrucommon.la

TESTS = BloomTests CircuitBreakerTests HashingTests SpreadTest 

check_PROGRAMS=$(TESTS)

BloomTests_SOURCES = BloomTests.cpp
BloomTests_LDADD   = $(LDADDS)
BloomTests_CXXFLAGS= $(CPPUNIT_CFLAGS) -I../src
BloomTests_LDFLAGS = $(CPPUNIT_LIBS)

CircuitBreakerTests_SOURCES = CircuitBreakerTests.cpp
CircuitBreakerTests_LDADD = $(LDADDS)
CircuitBreakerTests_CXXFLAGS =  $(CPPUNIT_CFLAGS) -I../src
CircuitBreakerTests_LDFLAGS = $(CPPUNIT_LIBS)

HashingTests_SOURCES = HashingTest.cpp
HashingTests_LDADD = $(LDADDS)
HashingTests_CXXFLAGS =  $(CPPUNIT_CFLAGS) -I../src
HashingTests_LDFLAGS = $(CPPUNIT_LIBS)

SpreadTest_SOURCES = SpreadTest.cpp
SpreadTest_LDADD = $(LDADDS)
SpreadTest_CXXFLAGS =  $(CPPUNIT_CFLAGS) -I../src
SpreadTest_LDFLAGS = $(CPPUNIT_LIBS) $(SPEAD_LIBS) $(THRIFT_LIBS) 

