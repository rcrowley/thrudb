# Makefile for test
# 
# Author:
#   T Jake Luciani <jakers@facebook.com>

# Default target is everything
target: all

# Tools
THRIFT = /usr/local/bin/thrift

all: ../../Thrudoc.thrift
	$(THRIFT) -perl ../../Thrudoc.thrift

clean:
	rm -fr gen-perl
