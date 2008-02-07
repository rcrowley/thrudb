# Makefile for test
# 
# Author:
#   T Jake Luciani <jake@3.rdrail.net>

# Default target is everything
target: all

# Tools
THRIFT = `which thrift`

all: ../src.new/Thrudex.thrift
	$(THRIFT) -perl ../src.new/Thrudex.thrift

clean:
	rm -fr gen-perl
