#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use Time::HiRes qw(time);

#Thrift components
use Thrift;
use Thrift::MemoryBuffer;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::FramedTransport;

use DistStore;

use constant DISTSTORE_PORT    => 9091;

use threads;

my $num_threads = 2;
my $num_calls = 500;

# load up data
eval{
    my $socket    = new Thrift::Socket("localhost",DISTSTORE_PORT());
    my $transport = new Thrift::FramedTransport($socket);
    my $protocol  = new Thrift::BinaryProtocol($transport);

    my $diststore  = new DistStoreClient($protocol);

    $transport->open();

    foreach (0..($num_calls - 1))
    {
        $diststore->put ("data", "key.$_", "value_$_");
    }
};
if($@) {
    die Dumper ($@) if UNIVERSAL::isa($@,"Thrift::TException");
    die $@;
}

my @threads = ();
foreach (0..($num_threads - 1))
{
    push @threads, threads->new (\&make_calls);
}

my $start = time;
foreach (@threads)
{
    $_->join;
}
my $end = time;
my $dur = $end - $start;
printf "%f -> %f = %f\n", $start, $end, $dur;
my $total_calls = $num_threads * $num_calls * 10;
my $tps = $total_calls / $dur;
printf "%d / %f = %f\n", $total_calls, $dur, $tps;


sub make_calls
{
    eval{
        my $socket    = new Thrift::Socket("localhost",DISTSTORE_PORT());
        my $transport = new Thrift::FramedTransport($socket);
        my $protocol  = new Thrift::BinaryProtocol($transport);

        my $diststore  = new DistStoreClient($protocol);

        $transport->open();

        foreach (0..(($num_calls - 1) * 10))
        {
            $diststore->get ("data", 'key.'.int (rand ($num_calls)));
        }
    };
    if($@) {
        die Dumper ($@) if UNIVERSAL::isa($@,"Thrift::TException");
        die $@;
    }
}

