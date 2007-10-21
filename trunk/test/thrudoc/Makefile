# Makefile for test
# 
# Author:
#   T Jake Luciani <jakers@facebook.com>

# Default target is everything
target: all

# Tools
THRIFT = /usr/local/bin/thrift

all: ../Service.Thrift
	$(THRIFT) -perl ../Service.Thrift

clean:
	rm -fr gen-perl
