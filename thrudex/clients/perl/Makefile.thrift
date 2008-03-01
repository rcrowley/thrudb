# Default target is everything
target: all

# Tools
THRIFT=`which thrift`

all: ../../src/Thrudex.thrift
	$(THRIFT) -perl ../../src/Thrudex.thrift
	mv gen-perl lib
clean:
	rm -fr lib
