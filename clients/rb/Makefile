THRIFT=`which thrift`

all:
	${THRIFT} -rb ../../Thrudoc.thrift
	${THRIFT} -rb ../../Thrucene.thrift
	mv gen-rb lib

clean:
	rm -fr gen-rb lib *~

