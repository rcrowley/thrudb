#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;

#Thrift components
use Thrift;
use Thrift::MemoryBuffer;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::FramedTransport;

#Thrudb
use MyTable;

#Config
use constant MYTABLE_PORT    => 9091;

use threads;

my @threads = ();
foreach (0..10)
{
    push @threads, threads->new (\&make_calls);
}

foreach (@threads)
{
    $_->join;
}

sub make_calls
{
    my $thrudoc;
    eval{
        my $socket    = new Thrift::Socket("localhost",MYTABLE_PORT());
        my $transport = new Thrift::FramedTransport($socket);
        my $protocol  = new Thrift::BinaryProtocol($transport);

        $thrudoc  = new MyTableClient($protocol);

        $transport->open();
    }; if($@) {
        die $@->{message} if UNIVERSAL::isa($@,"Thrift::TException");
        die $@;
    }

    eval {

        my $id = $thrudoc->put ("partitions", "key3", "val-key4");
        my $key = "key.".rand;
        $thrudoc->put ("partitions", $key, "val.$key");

        print Dumper ($thrudoc->get ("partitions", "key3"));
        print Dumper ($thrudoc->get ("partitions", $key));

        if (rand (100) > 50)
        {
            $thrudoc->remove ("partitions", $key);
        }

        print Dumper ($thrudoc->get ("partitions", "key3"));
#        print Dumper ($thrudoc->get ("partitions", $key));

        foreach (0..100)
        {
            $thrudoc->put ("partitions", "key.$_", "value_$_");
            $thrudoc->get ("partitions", "key.$_");
        }
    };
    if($@) {
        die Dumper ($@) if UNIVERSAL::isa($@,"Thrift::TException");
        die $@;
    }
}

