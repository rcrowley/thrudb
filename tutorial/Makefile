THRIFT=`which thrift`

all:
	${THRIFT} -perl -php -py -rb -java -cpp ../Thrudoc.thrift
	${THRIFT} -perl -php -py -rb -java -cpp ../Thrucene.thrift
	${THRIFT} -perl -php -py -rb -java -cpp Bookmark.thrift

clean:
	rm -fr gen-*
	rm *~

