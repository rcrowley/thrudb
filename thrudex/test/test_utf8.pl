#!/usr/bin/perl
use strict;
use warnings;

use Thrift;
use Thrift::MemoryBuffer;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::FramedTransport;

use lib "gen-perl";

use Thrudex;

my $socket    = new Thrift::Socket("localhost", 9099);
my $transport = new Thrift::FramedTransport($socket);
my $protocol  = new Thrift::BinaryProtocol($transport);
my $thrudex = new ThrudexClient($protocol);

$transport->open();

$thrudex->admin("create_index","bookmarks");

my $doc = new Thrudex::Document();

$doc->key('whatever');
$doc->index ( 'bookmarks');
my $field = new Thrudex::Field();
$field->key('title');
$field->value("Panasonic Youth \x{2013}   Learn Git 10 Different Ways");
push(@{$doc->{fields}}, $field);
eval {
       $thrudex->put( $doc );
};

print $@ if $@;
