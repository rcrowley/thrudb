# Makefile for test
# 
# Author:
#   T Jake Luciani <jake@3.rdrail.net>

# Default target is everything
target: all

# Tools
THRIFT = /usr/local/bin/thrift

all: ../../Thrucene.thrift
	$(THRIFT) -perl ../../Thrucene.thrift

clean:
	rm -fr gen-perl
