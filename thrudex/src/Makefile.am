bin_PROGRAMS = thrudex

thrudex_SOURCES = \
		  gen-cpp/Thrudex.cpp			\
		  gen-cpp/Thrudex_types.cpp		\
		  gen-cpp/Thrudex_constants.cpp		\
		  ThrudexHandler.cpp			\
		  ThrudexBackend.cpp			\
		  CLuceneBackend.cpp			\
		  CLuceneRAMDirectory.cpp               \
		  CLuceneIndex.cpp			\
		  main.cpp

thrudex_CPPFLAGS = -Wall -Igen-cpp $(MEMCACHED_CFLAGS) $(CURL_CFLAGS) $(THRIFTNB_CFLAGS) $(THRUCOMMON_CFLAGS) $(SSL_CFLAGS) $(UUID_CFLAGS)
thrudex_LDFLAGS = -Wall $(CURL_LIBS) $(THRIFTNB_LIBS) $(THRUCOMMON_LIBS) $(SSL_LIBS) $(UUID_LIBS) -levent -lclucene -lboost_filesystem

gen-cpp/Thrudex.cpp: Thrudex.thrift
	${THRIFT} -cpp $<

clean-local:
	rm -fr gen-cpp
