bin_PROGRAMS = thrudoc thrucene thrudoc_bench thruqueue

thrudoc_OBJDIR = obj

thrudoc_SOURCES=  gen-cpp/Thrudoc.cpp \
                  gen-cpp/Thrudoc_types.cpp \
                  gen-cpp/Thrudoc_constants.cpp \
		  gen-cpp/Redo.cpp \
		  gen-cpp/Redo_types.cpp \
		  gen-cpp/Redo_constants.cpp \
                  src/thrudoc/s3_glue.cpp \
		  src/thrudoc/base64.cpp \
		  src/thrudoc/main.cpp \
                  src/common/ConfigFile.cpp \
                  src/thrudoc/BloomManager.cpp \
                  src/thrudoc/ThrudocHandler.cpp \
		  src/thrudoc/ThrudocDiskBackend.cpp \
	          src/thrudoc/ThrudocDiskSpreadTask.cpp \
		  src/thrudoc/ThrudocS3Backend.cpp \
	          src/thrudoc/ThrudocS3SpreadTask.cpp \
		  src/thrudoc/WriteThroughS3Manager.cpp \
		  src/thrudoc/ThrudocWriteThroughBackend.cpp \
		  src/thrudoc/ThrudocWriteThroughSpreadTask.cpp \
                  src/common/SpreadManager.cpp \
		  src/common/RecoveryManager.cpp \
                  src/common/TransactionManager.cpp \
		  src/common/Transaction.cpp 

thrudoc_CXXFLAGS = -Wall -I/usr/local/include/thrift -I/usr/include/thrift -Isrc/common -Igen-cpp -Isrc/thrudoc
thrudoc_LDFLAGS = -Wall -L/usr/local/lib -luuid -lthrift -lthriftnb -levent -ltspread-core -llog4cxx -lmemcached

thrudoc_bench_SOURCES=  gen-cpp/Thrudoc.cpp \
			gen-cpp/Thrudoc_types.cpp \
			gen-cpp/Thrudoc_constants.cpp \
			src/thrudoc/bench.cpp

thrudoc_bench_CXXFLAGS = -Wall -I/usr/local/include/thrift -I/usr/include/thrift -Isrc -Igen-cpp -Isrc/common -Isrc/thrudoc
thrudoc_bench_LDFLAGS = -Wall -L/usr/local/lib -luuid -lthrift -levent -ltspread-core -llog4cxx


###########################################################################


thrucene_SOURCES= gen-cpp/Thrucene.cpp \
                  gen-cpp/Thrucene_types.cpp \
                  gen-cpp/Thrucene_constants.cpp \
		  gen-cpp/Redo.cpp \
		  gen-cpp/Redo_types.cpp \
		  gen-cpp/Redo_constants.cpp \
		  src/thrucene/main.cpp \
                  src/common/ConfigFile.cpp \
                  src/thrucene/ThruceneHandler.cpp \
		  src/thrucene/ThruceneSpreadTask.cpp \
		  src/thrucene/LuceneManager.cpp \
		  src/thrucene/LOG4CXX.cpp \
                  src/common/SpreadManager.cpp \
		  src/common/RecoveryManager.cpp \
                  src/common/TransactionManager.cpp \
		  src/common/Transaction.cpp 

thrucene_CXXFLAGS = -Wall -I/usr/local/include/thrift -I/usr/include/thrift -Isrc/common -Igen-cpp -Isrc/thrucene
thrucene_LDFLAGS = -Wall -L/usr/local/lib -luuid -lthrift -lthriftnb -levent -ltspread-core -llog4cxx -lmemcached

#thrucene_bench_SOURCES=  gen-cpp/Thrudoc.cpp \
#			gen-cpp/Thrudoc_types.cpp \
#			gen-cpp/Thrudoc_constants.cpp \
#			src/thrudoc/bench.cpp

#thrudoc_bench_CXXFLAGS = -Wall -I/usr/local/include/thrift -I/usr/include/thrift -Isrc -Igen-cpp -Isrc/common -Isrc/thrudoc
#thrudoc_bench_LDFLAGS = -Wall -L/usr/local/lib -luuid -lthrift -lthrift -levent -ltspread-core -llog4cxx

###########################################################################

thruqueue_SOURCES= gen-cpp/Thruqueue.cpp \
                  gen-cpp/Thruqueue_types.cpp \
                  gen-cpp/Thruqueue_constants.cpp \
		  gen-cpp/QueueLog.cpp \
		  src/thruqueue/QueueManager.cpp \
		  src/thruqueue/Queue.cpp \
		  src/thruqueue/ThruqueueHandler.cpp \
		  src/thruqueue/main.cpp \
                  src/common/ConfigFile.cpp 

thruqueue_CXXFLAGS = -Wall -I/usr/local/include/thrift -I/usr/include/thrift -Isrc/common -Igen-cpp -Isrc/thrucene
thruqueue_LDFLAGS = -Wall -L/usr/local/lib -luuid -lthrift -lthriftnb -levent -ltspread-core -llog4cxx -lmemcached


###########################################################################

gen-cpp/Thrudoc.cpp:
	${THRIFT} -cpp Thrudoc.thrift

gen-cpp/Redo.cpp:
	${THRIFT} -cpp Redo.thrift

gen-cpp/Thrucene.cpp:
	${THRIFT} -cpp Thrucene.thrift

gen-cpp/Thruqueue.cpp:
	${THRIFT} -cpp Thruqueue.thrift

clean-local:
	rm -fr gen-cpp

install-exec-hook:
	cp -r $(top_srcdir)/lib ${libdir}
	cp -r $(top_srcdir)/scripts ${prefix}

