bin_PROGRAMS = thruqueue

thruqueue_SOURCES = \
		  gen-cpp/QueueLog.cpp			\
		  gen-cpp/Thruqueue.cpp			\
		  gen-cpp/Thruqueue_types.cpp		\
		  gen-cpp/Thruqueue_constants.cpp	\
		  Queue.cpp				\
		  QueueManager.cpp			\
		  ThruqueueHandler.cpp			\
		  main.cpp

thruqueue_CPPFLAGS = -Wall -Igen-cpp $(CURL_CFLAGS) $(MEMCACHED_CFLAGS) $(THRIFTNB_CFLAGS) $(THRUCOMMON_CFLAGS) $(SSL_CFLAGS) $(UUID_CFLAGS)
thruqueue_LDFLAGS = -Wall $(CURL_LIBS) $(MEMCACHED_LIBS) $(THRIFTNB_LIBS) $(THRUCOMMON_LIBS) $(SSL_LIBS) $(UUID_LIBS)

gen-cpp/Thruqueue.cpp: Thruqueue.thrift
	${THRIFT} -cpp $<

clean-local:
	rm -fr gen-cpp
