THRIFT=`which thrift`

all:
	${THRIFT} -perl -php -rb -java ../Thrudoc.thrift
	${THRIFT} -perl -php -rb -java ../Thrucene.thrift
	${THRIFT} -perl -php -rb -java Bookmark.thrift

clean:
	rm -fr gen-*
	rm *~

