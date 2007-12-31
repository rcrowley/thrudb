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

    my $obj = new Obj (
        foo => 'bar',
        baz => 1,
    );
    print Dumper ($obj);
    my $ser = $obj->save;
    print Dumper ($ser);
    my $rev = Obj::load ($ser);
    print Dumper ($rev, $rev->get ('foo'), $rev->get ('baz'));

    # now through diststore
    $diststore->put ("partitions", "obj", $ser);
    $rev = Obj::load ($diststore->get ("partitions", "obj"));
    print Dumper ($rev, $rev->get ('foo'), $rev->get ('baz'));
};
if($@) {
    die Dumper ($@) if UNIVERSAL::isa($@,"Thrift::TException");
    die $@;
}

package Obj;

use strict;
use warnings;
use Data::Dumper;

# class static

sub new
{
    my $class = shift;
    my @params = @_;
    for (my $i = 0; $i < scalar (@params); $i += 2)
    {
        printf STDERR "%s -> %s\n", $params[$i], '_'.$params[$i];
        $params[$i] = '_'.$params[$i];
    }
    return bless { @params }, $class;
}

sub load
{
    my ($data) = @_;
    my $VAR1 = undef;
    eval $data;
    if($@) {
        die $@;
    }
    return $VAR1;
}

# methods

sub save
{
    my ($self) = @_;
    local $Data::Dumper::Purity = 1;
    local $Data::Dumper::Indent = 0;
    return Dumper ($self);
}

sub get
{
    my ($self, $key) = @_;
    return $self->{"_$key"};
}

sub put
{
    my ($self, $key, $val) = @_;
    $self->{"_$key"} = $val;
}
