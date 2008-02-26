# TODO: make sure we link against our thrucommon and not an installed one

INCLUDES = -I$(top_builddir)/src
LDADDS = ../src/libthrucommon.la

TESTS = SpreadTest CircuitBreakerTest

check_PROGRAMS=$(TESTS)

SpreadTest_SOURCES = SpreadTest.cpp
SpreadTest_LDADD = $(LDADDS)
SpreadTest_CXXFLAGS =  $(CPPUNIT_CFLAGS) -I../src
SpreadTest_LDFLAGS = $(CPPUNIT_LIBS) $(SPEAD_LIBS) $(THRIFT_LIBS) 

CircuitBreakerTest_SOURCES = CircuitBreakerTest.cpp
CircuitBreakerTest_LDADD = $(LDADDS)
CircuitBreakerTest_CXXFLAGS =  $(CPPUNIT_CFLAGS) -I../src
CircuitBreakerTest_LDFLAGS = $(CPPUNIT_LIBS)
