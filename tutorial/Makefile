THRIFT=`which thrift`

all:
	${THRIFT} -perl -php -rb -java -py ../thrudoc/src/Thrudoc.thrift
	${THRIFT} -perl -php -rb -java -py ../thrudex/src/Thrudex.thrift
	${THRIFT} -perl -php -rb -java -py Bookmark.thrift

clean:
	rm -fr gen-*
	rm *~

