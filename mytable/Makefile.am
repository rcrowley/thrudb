bin_PROGRAMS = mytable

mytable_OBJDIR = obj

mytable_SOURCES = \
		  gen-cpp/MyTable.cpp		\
		  gen-cpp/MyTable_types.cpp	\
		  gen-cpp/MyTable_constants.cpp	\
		  src/ConfigFile.cpp		\
		  src/MyTableHandler.cpp	\
		  src/MySQLBackend.cpp		\
		  src/mysql_glue.cpp		\
		  src/main.cpp

mytable_CXXFLAGS = -Wall -I$(SANDBOX)/include -I$(SANDBOX)/include/thrift -I/usr/local/include/thrift -I/usr/include/thrift -I/usr/include/mysql -Igen-cpp 

mytable_LDFLAGS = -Wall -L$(SANDBOX)/lib -L/usr/local/lib -lthrift -lthriftnb -levent -llog4cxx -lmysqlclient_r

gen-cpp/MyTable.cpp: MyTable.thrift
	${THRIFT} -cpp MyTable.thrift

clean-local:
	rm -fr gen-cpp

#install-exec-hook:
#	cp -r $(top_srcdir)/lib ${libdir}
#	cp -r $(top_srcdir)/scripts ${prefix}

