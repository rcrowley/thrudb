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

use DistStore;

#Config
use constant DISTSTORE_PORT    => 9091;

my $diststore;
eval{
    my $socket    = new Thrift::Socket("localhost",DISTSTORE_PORT());
    my $transport = new Thrift::FramedTransport($socket);
    my $protocol  = new Thrift::BinaryProtocol($transport);

    $diststore  = new DistStoreClient($protocol);

    $transport->open();
}; if($@) {
    die $@->{message} if UNIVERSAL::isa($@,"Thrift::TException");
    die $@;
}

eval {

    my $tablename = 'data';

    my $id = $diststore->put ($tablename, "key3", "val-key4");
    my $key = "key.".rand;
    $diststore->put ($tablename, $key, "val.$key");

    print Dumper ($diststore->get ($tablename, "key3"));
    print Dumper ($diststore->get ($tablename, $key));

    if (rand (100) > 50)
    {
        $diststore->remove ($tablename, $key);
    }
    else
    {
        print Dumper ($diststore->get ($tablename, $key));
    }

    print Dumper ($diststore->get ($tablename, "key3"));

    my $batch_size = 13;
    my $ret = $diststore->scan ($tablename, undef, $batch_size);
    my $count = 0;
    while (scalar (@{$ret->{elements}}) > 0)
    {
        printf "seed: %s - %d\n", $ret->{seed}, scalar (@{$ret->{elements}});
        foreach (@{$ret->{elements}})
        {
            printf "\t%s => %s\n", $_->{key}, $_->{value};
        }
        $count += scalar (@{$ret->{elements}});
        $ret = $diststore->scan ($tablename, $ret->{seed}, $batch_size);
        $diststore->remove ($tablename, $ret->{seed});
    }
    printf "count: %d\n", $count;
};
if($@) {
    die Dumper ($@) if UNIVERSAL::isa($@,"Thrift::TException");
    die $@;
}
