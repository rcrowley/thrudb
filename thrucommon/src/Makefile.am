
pkgconfigdir = $(libdir)/pkgconfig

lib_LTLIBRARIES = libthrucommon.la
pkgconfig_DATA = thrucommon.pc

include_thrucommondir = $(includedir)/thrucommon
include_thrucommon_HEADERS = \
			     gen-cpp/Redo.h		\
			     gen-cpp/Redo_types.h	\
			     gen-cpp/EventLog.h		\
			     gen-cpp/EventLog_types.h	\
			     ConfigFile.h		\
			     memcache++.h		\
			     MemcacheHandle.h		\
			     ThruFileTransport.h	\
			     RecoveryManager.h		\
			     SpreadManager.h		\
			     SpreadTaskFactory.h	\
			     Transaction.h		\
			     TransactionManager.h	\
			     utils.h

libthrucommon_la_SOURCES = \
			   gen-cpp/Redo.cpp			\
			   gen-cpp/Redo_types.cpp		\
			   gen-cpp/Redo_constants.cpp		\
			   gen-cpp/EventLog.cpp			\
			   gen-cpp/EventLog_types.cpp		\
			   gen-cpp/EventLog_constants.cpp	\
			   ConfigFile.cpp			\
			   ThruFileTransport.cpp		\
			   RecoveryManager.cpp			\
			   SpreadManager.cpp			\
			   Transaction.cpp			\
			   TransactionManager.cpp

libthrucommon_la_CPPFLAGS = -Wall -Igen-cpp $(MEMCACHED_CFLAGS) $(SPEAD_CFLAGS) $(SSL_CFLAGS) $(THRIFT_CFLAGS) $(UUID_CFLAGS)
libthrucommon_la_LDFLAGS = -Wall $(SPEAD_LIBS) $(SSL_LIBS) $(THRIFT_LIBS) $(UUID_LIBS) $(MEMCACHED_LIBS)

gen-cpp/Redo.cpp: Redo.thrift
	${THRIFT} -cpp $<

gen-cpp/EventLog.cpp: EventLog.thrift
	${THRIFT} -cpp $<

clean-local:
	rm -fr gen-cpp
