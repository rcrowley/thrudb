SUBDIRS = src examples

# test section taken from a Makefile.PL run...
FULLPERL      = /usr/bin/perl
FULLPERLRUN   = $(FULLPERL)
TEST_VERBOSE=0
TEST_TYPE=test_$(LINKTYPE)
TEST_FILE = test.pl
TEST_FILES = t/*.t
TESTDB_SW = -d

test: all
	PERL5LIB=$(PERL5LIB):examples/gen-perl PERL_DL_NONLAZY=1 $(FULLPERLRUN) "-MExtUtils::Command::MM" "-e" "test_harness($(TEST_VERBOSE), '$(INST_LIB)', '$(INST_ARCHLIB)')" $(TEST_FILES)

#rpm: all dist
#	cp libmemcached-$(VERSION).tar.gz /home/brian/rpmbuild/SOURCES/
#	rpmbuild -ba support/libmemcached.spec
#	cp /home/brian/rpmbuild/RPMS/x86_64/libmemcached-$(VERSION)*.rpm .
#	cp /home/brian/rpmbuild/SRPMS/libmemcached-$(VERSION)*.rpm .
