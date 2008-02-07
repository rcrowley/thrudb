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

my $index  = "jddev";

eval{
    $transport->open();
    query_it();
    warn "Adding data\n";
    add_some_data();


    warn "Querying data\n";
    query_it();

    warn "Updating data\n";
    update_it();

    warn "Querying data\n";
    query_it();

    warn "Deleting data\n";
    delete_it();


    warn "Querying data\n";
    query_it();

    $transport->close();

}; if($@){
    warn(Dumper($@));
}


sub add_some_data
{
    my @data;
    foreach my $i (1..100){

        my $d = new Thrudex::Document();


        $d->index(  $index );
        $d->key( $i );

        foreach my $j (1..10){

            my $field = new Thrudex::Field();
            $field->key( "name".$j );
            $field->value( "value".$i.$j);

            push(@{$d->{fields}}, $field);
        }

        $client->put( $d );
    }


}


sub update_it
{
    my @data;
    foreach my $i (1..100){

        my $d = new Thrudex::Document();

        $d->index( $index);

        $d->key( $i );

        foreach my $j (1..10){

            my $field = new Thrudex::Field();
            $field->{key}  = "name".$j;
            $field->{value} = "value update".$i.$j;

            push(@{$d->{fields}}, $field);
        }


        $client->put( $d );

    }

}


sub query_it
{
    my $q = new Thrudex::SearchQuery();

    $q->{index}  = $index;
    $q->{query}  = qq{+name1:(value)};
    #$q->{limit}  = 10;
    my $r = $client->search( $q );

    warn(Dumper($r));
}

sub delete_it
{
    my @data;
    foreach my $i (1..100){


        my $rm = new Thrudex::Element();

        $rm->{index}  = $index;

        $rm->{key}= $i;

        $client->remove($rm);
    }

}

