# Makefile for test
# 
# Author:
#   T Jake Luciani <jake@3.rdrail.net>

# Default target is everything
target: all

# Tools
THRIFT = `which thrift`

all: ../src/Thrudex.thrift
	$(THRIFT) -perl -rb ../src/Thrudex.thrift

clean:
	rm -fr gen-perl gen-rb
