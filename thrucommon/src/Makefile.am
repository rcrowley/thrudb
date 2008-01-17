
pkgconfigdir = $(libdir)/pkgconfig

lib_LTLIBRARIES = libthrucommon.la
pkgconfig_DATA = thrucommon.pc

include_thrucommondir = $(includedir)/thrucommon
include_thrucommon_HEADERS = \
			     ConfigFile.h

libthrucommon_la_SOURCES = \
			   gen-cpp/Redo.cpp		\
			   gen-cpp/Redo_types.cpp	\
			   gen-cpp/Redo_constants.cpp	\
			   ConfigFile.cpp		\
			   RecoveryManager.cpp		\
			   SpreadManager.cpp		\
			   Transaction.cpp		\
			   TransactionManager.cpp

libthrucommon_la_CPPFLAGS = -Wall -Igen-cpp $(CURL_CFLAGS) $(MEMCACHED_CFLAGS) $(THRIFT_CFLAGS) $(SSL_CFLAGS) $(UUID_CFLAGS)
libthrucommon_la_LDFLAGS = -Wall $(CURL_LIBS) $(MEMCACHED_LIBS) $(THRIFT_LIBS) $(SSL_LIBS) $(UUID_LIBS)

gen-cpp/Redo.cpp: Redo.thrift
	${THRIFT} -cpp $<

clean-local:
	rm -fr gen-cpp
