bin_PROGRAMS = thrudoc thrucene thrudoc_bench 

OBJDIR = obj

thrudoc_SOURCES=  gen-cpp/Thrudoc.cpp \
                  gen-cpp/Thrudoc_types.cpp \
                  gen-cpp/Thrudoc_constants.cpp \
		  gen-cpp/Redo.cpp \
		  gen-cpp/Redo_types.cpp \
		  gen-cpp/Redo_constants.cpp \
		  src/common/memcache++.cpp \
		  src/thrudoc/bloom_filter.cpp \
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
                  src/common/SpreadManager.cpp \
		  src/common/RecoveryManager.cpp \
                  src/common/TransactionManager.cpp \
		  src/common/Transaction.cpp 

thrudoc_CXXFLAGS = -Wall -I/usr/local/include/thrift -I/usr/include/thrift -Isrc/common -Igen-cpp -Isrc/thrudoc
thrudoc_LDFLAGS = -Wall -L/usr/local/lib -luuid -lthrift -lthriftnb -levent -ltspread-core -llog4cxx

thrudoc_bench_SOURCES=  gen-cpp/Thrudoc.cpp \
			gen-cpp/Thrudoc_types.cpp \
			gen-cpp/Thrudoc_constants.cpp \
			src/thrudoc/bench.cpp

thrudoc_bench_CXXFLAGS = -Wall -I/usr/local/include/thrift -I/usr/include/thrift -Isrc -Igen-cpp -Isrc/common -Isrc/thrudoc
thrudoc_bench_LDFLAGS = -Wall -L/usr/local/lib -luuid -lthrift -lthrift -levent -ltspread-core -llog4cxx


###########################################################################


thrucene_SOURCES= gen-cpp/Thrucene.cpp \
                  gen-cpp/Thrucene_types.cpp \
                  gen-cpp/Thrucene_constants.cpp \
		  gen-cpp/Redo.cpp \
		  gen-cpp/Redo_types.cpp \
		  gen-cpp/Redo_constants.cpp \
		  src/common/memcache++.cpp \
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
thrucene_LDFLAGS = -Wall -L/usr/local/lib -luuid -lthrift -lthriftnb -levent -ltspread-core -llog4cxx

#thrucene_bench_SOURCES=  gen-cpp/Thrudoc.cpp \
#			gen-cpp/Thrudoc_types.cpp \
#			gen-cpp/Thrudoc_constants.cpp \
#			src/thrudoc/bench.cpp

#thrudoc_bench_CXXFLAGS = -Wall -I/usr/local/include/thrift -I/usr/include/thrift -Isrc -Igen-cpp -Isrc/common -Isrc/thrudoc
#thrudoc_bench_LDFLAGS = -Wall -L/usr/local/lib -luuid -lthrift -lthrift -levent -ltspread-core -llog4cxx

###########################################################################

gen-cpp/Thrudoc.cpp:
	${THRIFT} -cpp Thrudoc.thrift

gen-cpp/Redo.cpp:
	${THRIFT} -cpp Redo.thrift

gen-cpp/Thrucene.cpp:
	${THRIFT} -cpp Thrucene.thrift

clean-local:
	rm -fr gen-cpp
