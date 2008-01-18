#
#
#

CONFIGURE_OPTS = --prefix=$(SANDBOX)

all: thrucommon thrudex thrudoc thruqueue

thrucommon: thrucommon/Makefile
	(cd $@ && make all install)

thrudex: thrudex/Makefile
	(cd $@ && make all install)

thrudoc: thrudoc/Makefile
	(cd $@ && make all install)

thruqueue: thruqueue/Makefile
	(cd $@ && make all install)

%/Makefile: %/Makefile.in
	(cd $(@:/Makefile=) && ./configure $(CONFIGURE_OPTS))

%/Makefile.in: %/Makefile.am
	(cd $(@:/Makefile.in=) && ./bootstrap.sh)

.PRECIOUS: %/Makefile %/Makefile.in
.PHONY: thrucommon thrudex thrudoc thruqueue
