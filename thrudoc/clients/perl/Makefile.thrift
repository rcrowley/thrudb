# Default target is everything
target: all

# Tools
THRIFT=`which thrift`

all: ../../src/Thrudoc.thrift
	$(THRIFT) -perl ../../src/Thrudoc.thrift
	mv gen-perl lib
clean:
	rm -fr lib
