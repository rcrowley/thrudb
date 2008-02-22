#!/usr/bin/perl

# example graph command:
# rrdtool graph req.png --end 1202197140 --start 1202194500 --height=600    \
#   --width=800 DEF:gets=animal.rrd:get_count:AVERAGE LINE2:gets#ff0000   \
#   DEF:puts=animal.rrd:put_count:AVERAGE LINE2:puts#0000ff   \
#   DEF:scan=animal.rrd:scan_count:AVERAGE LINE2:scan#00ff00  \

use strict;
use warnings;
use Data::Dumper;
use Thrift;
use Thrift::MemoryBuffer;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::FramedTransport;
use Thrudoc;

my $hostname = shift @ARGV;
my $port = 9092;
my $interval = 60;

my $thrudoc;
eval
{
    my $socket    = new Thrift::Socket ($hostname, $port);
    my $transport = new Thrift::FramedTransport ($socket);
    my $protocol  = new Thrift::BinaryProtocol ($transport);

    $thrudoc  = new ThrudocClient($protocol);

    $transport->open();

    create ($hostname, $interval * 2, get_stats ());
}; 
if($@) 
{
    die $@->{message} if (UNIVERSAL::isa ($@,"Thrift::TException"));
    die $@;
}

while (1)
{
    eval
    {
        update ($hostname, get_stats ());
    }; 
    if($@) 
    {
        if (UNIVERSAL::isa($@,"Thrift::TException"))
        {
            warn $@->{message};
        }
        else
        {
            die $@;
        }
    }
    sleep $interval;
}

sub get_stats
{
    my $stats = $thrudoc->admin ('stats');
    $stats = [ split /,/, $stats ];
    $stats = { map { split /=/ } @$stats };
}

sub create
{
    my ($hostname, $heartbeat, $stats) = @_;

    my @dss = ();
    foreach my $var (sort keys %$stats)
    {
        push @dss, sprintf ('DS:%s:COUNTER:%d:0:U', $var, $heartbeat);
    }

    my $cmd = sprintf ('rrdtool create rrdtool/%s.rrd --step 60 %s %s', $hostname,
        join (' ', @dss), 'RRA:AVERAGE:0.5:1:60 RRA:AVERAGE:0.5:5:72 RRA:AVERAGE:0.5:60:24 RRA:AVERAGE:0.5:1440:7');
    print $cmd."\n";
    system ($cmd);
}

sub update
{
    my ($hostname, $stats) = @_;
    my $cmd = sprintf ('rrdtool update rrdtool/%s.rrd N:%s', $hostname, 
        join (':', map { $stats->{$_} } sort keys %$stats));
    print $cmd."\n";
    system ($cmd);
}
