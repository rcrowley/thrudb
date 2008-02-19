bin_PROGRAMS = throxy

throxy_SOURCES = \
		  gen-cpp/Thrudoc.cpp			\
		  gen-cpp/Thrudoc_types.cpp		\
		  gen-cpp/Thrudoc_constants.cpp		\
		  gen-cpp/Thrudex.cpp			\
		  gen-cpp/Thrudex_types.cpp		\
		  gen-cpp/Thrudex_constants.cpp		\
		  ThrudocHandler.cpp			\
		  ServiceMonitor.cpp			\
		  StaticServiceMonitor.cpp              \
		  main.cpp

#SpreadMonitor.cpp
#

throxy_CPPFLAGS = -Wall -Igen-cpp $(MEMCACHED_CFLAGS) $(CURL_CFLAGS) $(THRIFTNB_CFLAGS) $(THRUCOMMON_CFLAGS) $(SSL_CFLAGS) $(UUID_CFLAGS)
throxy_LDFLAGS = -Wall $(CURL_LIBS) $(THRIFTNB_LIBS) $(THRUCOMMON_LIBS) $(SSL_LIBS) $(UUID_LIBS) -levent 

gen-cpp/Thrudex.cpp: ../../thrudex/src/Thrudex.thrift
	${THRIFT} -cpp $<

gen-cpp/Thrudoc.cpp: ../../thrudoc/src/Thrudoc.thrift
	${THRIFT} -cpp $<

clean-local:
	rm -fr gen-cpp
