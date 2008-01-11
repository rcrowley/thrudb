THRIFT=`which thrift`

all:
	${THRIFT} -perl -php -rb -java -py ../Thrudoc.thrift
	${THRIFT} -perl -php -rb -java -py ../Thrucene.thrift
	${THRIFT} -perl -php -rb -java -py Bookmark.thrift

clean:
	rm -fr gen-*
	rm *~

