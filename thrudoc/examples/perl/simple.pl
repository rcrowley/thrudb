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

use Thrudoc;

#Config
use constant THRUDOC_PORT    => 9091;

my $thrudoc;
eval{
    my $socket    = new Thrift::Socket("localhost",THRUDOC_PORT());
    my $transport = new Thrift::FramedTransport($socket);
    my $protocol  = new Thrift::BinaryProtocol($transport);

    $thrudoc  = new ThrudocClient($protocol);

    $transport->open();
}; if($@) {
    die $@->{message} if UNIVERSAL::isa($@,"Thrift::TException");
    die $@;
}

eval {

    my $tablename = 'data';

    my $id = $thrudoc->put ($tablename, "key3", "val-key4");
    my $key = "key.".rand;
    $thrudoc->put ($tablename, $key, "val.$key");

    print Dumper ($thrudoc->get ($tablename, "key3"));
    print Dumper ($thrudoc->get ($tablename, $key));

    if (rand (100) > 50)
    {
#        $thrudoc->remove ($tablename, $key);
    }
    else
    {
        print Dumper ($thrudoc->get ($tablename, $key));
    }

    print Dumper ($thrudoc->get ($tablename, "key3"));

    my $batch_size = 13;
    my $ret = $thrudoc->scan ($tablename, undef, $batch_size);
    my $count = 0;
    while (scalar (@{$ret->{elements}}) > 0)
    {
        printf "seed: %s - %d\n", $ret->{seed}, scalar (@{$ret->{elements}});
        foreach (@{$ret->{elements}})
        {
            printf "\t%s => %s\n", $_->{key}, $_->{value};
        }
        $count += scalar (@{$ret->{elements}});
        $ret = $thrudoc->scan ($tablename, $ret->{seed}, $batch_size);
    }
    printf "count: %d\n", $count;
};
if($@) {
    die Dumper ($@) if UNIVERSAL::isa($@,"Thrift::TException");
    die $@;
}
