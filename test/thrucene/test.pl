#!/usr/bin/perl

use strict;
use warnings;

use lib './gen-perl';

use Thrift;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::FramedTransport;

use Data::Dumper;

use Thrucene;

my $socket    = new Thrift::Socket('localhost',9099);
my $transport = new Thrift::FramedTransport($socket);
my $protocol  = new Thrift::BinaryProtocol($transport);
my $client    = new ThruceneClient($protocol);

my $domain = "test";
my $index  = "index1";

eval{
    $transport->open();
    query_it();
    warn "Adding data\n";
    add_some_data();

    $client->commitAll();

    warn "Querying data\n";
    query_it();

    warn "Updating data\n";
    update_it();

    $client->commitAll();

    warn "Querying data\n";
    query_it();

    warn "Deleting data\n";
    delete_it();

    $client->commitAll();

    warn "Querying data\n";
    query_it();

    $transport->close();

}; if($@){
    warn(Dumper($@));
}


sub add_some_data
{
    my @data;
    foreach my $i (1..10){

        my $d = new Thrucene::DocMsg();

        $d->domain( $domain );
        $d->index(  $index );
        $d->docid( $i );

        foreach my $j (1..10){

            my $field = new Thrucene::Field();
            $field->name( "name".$j );
            $field->value( "value".$i.$j);

            push(@{$d->{fields}}, $field);
        }

        push(@data,$d);
    }

    $client->addList( \@data );
}


sub update_it
{
    my @data;
    foreach my $i (1..10){

        my $d = new Thrucene::DocMsg();

        $d->{domain} = $domain;
        $d->{index}  = $index;

        $d->{docid}= $i;

        foreach my $j (1..10){

            my $field = new Thrucene::Field();
            $field->{name}  = "name".$j;
            $field->{value} = "value update".$i.$j;

            push(@{$d->{fields}}, $field);
        }


        push(@data,$d);
    }

    $client->updateList( \@data );


}


sub query_it
{
    my $q = new Thrucene::QueryMsg();

    $q->{domain} = $domain;
    $q->{index}  = $index;
    $q->{query}  = qq{+name1:("value*")};
    #$q->{limit}  = 10;
    my $r = $client->query( $q );

    warn(Dumper($r));
}

sub delete_it
{
    my @data;
    foreach my $i (1..10){

        my $rm = new Thrucene::RemoveMsg();

        $rm->{domain} = $domain;
        $rm->{index}  = $index;

        $rm->{docid}= $i;

        push(@data,$rm);
    }

    $client->removeList(\@data);

}

