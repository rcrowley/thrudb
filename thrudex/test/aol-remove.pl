#!/usr/bin/perl

use strict;
use warnings;

use lib './gen-perl';

use Thrift;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::FramedTransport;

use Data::Dumper;

use Thrudex;

my $socket    = new Thrift::Socket('localhost',9099);
my $transport = new Thrift::FramedTransport($socket);
my $protocol  = new Thrift::BinaryProtocol($transport);
my $client    = new ThrudexClient($protocol);

my $index  = shift;
my $query  = shift;

die "$0 index_name query\n" unless defined $index && defined $query;

eval{
    $transport->open();

    my $q = new Thrudex::Element();

    $q->{index}  = $index;

    my $i = 0;
    while($i<10000){
        $q->{key} = "../../../aol-data/user-ct-test-collection-02.txt".$i;

        my $r = $client->remove( $q );
        $i++;

    }


    $transport->close();

}; if($@){
    warn(Dumper($@));
}

