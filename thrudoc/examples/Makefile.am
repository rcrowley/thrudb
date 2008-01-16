
THRIFT=`which thrift`

all: gen-perl/Thrudoc.pm

gen-perl/Thrudoc.pm: $(top_srcdir)/src/Thrudoc.thrift
	${THRIFT} -cpp -java -php -py -rb -xsd -perl $<

clean:
	rm -fr gen-*
