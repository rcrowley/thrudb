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

use Time::HiRes qw(gettimeofday);

my $socket    = new Thrift::Socket('localhost',9099);
my $transport = new Thrift::FramedTransport($socket);
my $protocol  = new Thrift::BinaryProtocol($transport);
my $client    = new ThrudexClient($protocol);

my $index  = shift;
my $infile = shift;

die "$0 index_name aol-file.txt\n" unless defined $index && defined $infile;

open(INFILE,"<".$infile) || $!;

my $header;

chomp($header = <INFILE>);
my @header = split(/\t/,$header);

my $key = 1;
my $last_key = $key;
my $t0 = gettimeofday();

eval{
    $transport->open();

    $client->admin("create_index",$index);

    while(chomp(my $line = <INFILE>)){
        my @data = split(/\t/,$line);

        my $doc = new Thrudex::Document;
        $doc->key($infile.$key);
        $doc->index($index);

        for(my $j=0; $j<@header; $j++){
            next unless defined $data[$j] && $data[$j] ne "";

            my $field = new Thrudex::Field();
            $field->key( $header[$j] );
            $field->value( $data[$j] );

            push(@{$doc->{fields}}, $field);
        }

        $client->put($doc);

        my $t1 = gettimeofday();

        if(($t1 - $t0) > 1){

            warn(($key - $last_key)." indexed in ".sprintf("%0.2f",($t1-$t0))."sec, $key total\n");

            $t0       = $t1;
            $last_key = $key;

        }

        $key++;
    }

    $transport->close();

}; if($@){
    warn(Dumper($@));
}

