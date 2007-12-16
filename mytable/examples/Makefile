THRIFT=`which thrift`

all:
	${THRIFT} -cpp -java -php -py -rb -xsd -perl ../MyTable.thrift

clean:
	rm -fr gen-*
	rm *~

