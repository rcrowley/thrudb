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

    my $q = new Thrudex::SearchQuery();

    $q->{index}  = $index;
    $q->{query}  = $query;
    my $i = 0;
    while(1){
        my $r = $client->search( $q );
        $i++;

        if( $i % 100 == 0 ){
            print "Found ".$r->total."\n";
        }

    }


    $transport->close();

}; if($@){
    warn(Dumper($@));
}

