# Default target is everything
target: all

# Tools
THRIFT=/usr/local/bin/thrift

all: ../../../Thrudoc.thrift
	$(THRIFT) -perl ../../../Thrudoc.thrift
	mv gen-perl lib
clean:
	rm -fr lib
