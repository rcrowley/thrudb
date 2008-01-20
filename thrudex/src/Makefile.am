bin_PROGRAMS = thrudex
noinst_PROGRAMS = thrudex_play

thrudex_SOURCES = \
		  gen-cpp/Thrudex.cpp			\
		  gen-cpp/Thrudex_types.cpp		\
		  gen-cpp/Thrudex_constants.cpp		\
		  LOG4CXX.cpp				\
		  LuceneManager.cpp			\
		  ThrudexHandler.cpp			\
		  ThrudexSpreadTask.cpp			\
		  main.cpp

thrudex_CPPFLAGS = -Wall -Igen-cpp $(MEMCACHED_CFLAGS) $(CURL_CFLAGS) $(THRIFTNB_CFLAGS) $(THRUCOMMON_CFLAGS) $(SSL_CFLAGS) $(UUID_CFLAGS)
thrudex_LDFLAGS = -Wall $(CURL_LIBS) $(THRIFTNB_LIBS) $(THRUCOMMON_LIBS) $(SSL_LIBS) $(UUID_LIBS) -levent

thrudex_play_SOURCES = \
		  gen-cpp/Thrudex.cpp			\
		  gen-cpp/Thrudex_types.cpp		\
		  gen-cpp/Thrudex_constants.cpp		\
		  play.cpp

thrudex_play_CPPFLAGS = -Wall -Igen-cpp $(MEMCACHED_CFLAGS) $(CURL_CFLAGS) $(THRIFTNB_CFLAGS) $(THRUCOMMON_CFLAGS) $(SSL_CFLAGS) $(UUID_CFLAGS)
thrudex_play_LDFLAGS = -Wall $(CURL_LIBS) $(THRIFTNB_LIBS) $(THRUCOMMON_LIBS) $(SSL_LIBS) $(UUID_LIBS) -levent

gen-cpp/Thrudex.cpp: Thrudex.thrift
	${THRIFT} -cpp $<

clean-local:
	rm -fr gen-cpp
