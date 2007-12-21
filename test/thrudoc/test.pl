#!/usr/bin/perl

use strict;
use warnings;

use lib './gen-perl';

use Thrift;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::BufferedTransport;
use Thrift::FramedTransport;

use Data::Dumper;
use Time::HiRes qw(gettimeofday);
use Thrudoc;

my $socket    = new Thrift::Socket('localhost',9091);
my $transport = new Thrift::FramedTransport($socket);
my $protocol  = new Thrift::BinaryProtocol($transport);
my $client    = new ThrudocClient($protocol);

my $doc = qq{
    // Example document
    {
        "name":12345, // integer
        "color":"white",
        "type":"washer",
        "tstamp":12345 // long
    }
};

my $count = shift;

$transport->open();


my $t0 = gettimeofday();
for(my $i=0; $i<$count; $i++){
    my $id = $client->store($doc);
}
my $t1 = gettimeofday();

print "Added $count docs in ".sprintf("%0.2f",($t1-$t0))."Secs\n";

