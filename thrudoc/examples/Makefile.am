
THRIFT=`which thrift`

all: gen-perl/DistStore.pm

gen-perl/DistStore.pm: $(top_srcdir)/src/DistStore.thrift
	${THRIFT} -cpp -java -php -py -rb -xsd -perl $<

clean:
	rm -fr gen-*
