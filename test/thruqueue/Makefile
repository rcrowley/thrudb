# Makefile for test
# 
# Author:
#   T Jake Luciani <jake@3.rdrail.net>

# Default target is everything
target: all

# Tools
THRIFT = /usr/local/bin/thrift

all: ../../Thruqueue.thrift
	$(THRIFT) -perl ../../Thruqueue.thrift

clean:
	rm -fr gen-perl
