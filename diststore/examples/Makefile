THRIFT=`which thrift`

all:
	${THRIFT} -cpp -java -php -py -rb -xsd -perl ../DistStore.thrift

clean:
	rm -fr gen-*
	rm *~

