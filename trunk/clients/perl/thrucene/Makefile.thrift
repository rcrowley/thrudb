# Default target is everything
target: all

# Tools
THRIFT = /usr/local/bin/thrift

all: ../../../Thrucene.thrift
	$(THRIFT) -perl ../../../Thrucene.thrift
	mv gen-perl lib
clean:
	rm -fr lib
