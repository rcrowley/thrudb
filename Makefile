#
#
#

#CONFIGURE_OPTS = --prefix=$(SANDBOX)
PKGS=thrucommon thrudex thrudoc thruqueue

all: $(PKGS)

clean:
	for i in $(PKGS); do		\
	    (cd $$i && make $@);	\
	done

distclean:
	for i in $(PKGS); do		\
	    (cd $$i && make $@);	\
	done

distclean-am:
	for i in $(PKGS); do		\
	    (cd $$i && make $@);	\
	done

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
