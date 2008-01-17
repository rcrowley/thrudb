bin_PROGRAMS = thrudex

thrudex_SOURCES = \
		  gen-cpp/Thrucene.cpp			\
		  gen-cpp/Thrudex_types.cpp		\
		  gen-cpp/Thrudex_constants.cpp	\
		  LOG4CXX.cpp				\
		  LuceneManager.cpp			\
		  ThruceneHandler.cpp			\
		  ThruceneSpreadTask.cpp		\
		  main.cpp

thrudex_CPPFLAGS = -Wall -Igen-cpp $(CURL_CFLAGS) $(MEMCACHED_CFLAGS) $(THRIFTNB_CFLAGS) $(THRUCOMMON_CFLAGS) $(SSL_CFLAGS) $(UUID_CFLAGS)
thrudex_LDFLAGS = -Wall $(CURL_LIBS) $(MEMCACHED_LIBS) $(THRIFTNB_LIBS) $(THRUCOMMON_LIBS) $(SSL_LIBS) $(UUID_LIBS)

gen-cpp/Thrucene.cpp: Thrudex.thrift
	${THRIFT} -cpp $<

clean-local:
	rm -fr gen-cpp
