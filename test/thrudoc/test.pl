#!/usr/bin/perl

use strict;
use warnings;

use lib './gen-perl';

use Thrift;
use Thrift::BinaryProtocol;
use Thrift::Socket;
use Thrift::BufferedTransport;
use Thrift::FramedTransport;

use Data::Dumper;

use Service_types;
use Service_constants;
use Thrudb;

my $socket    = new Thrift::Socket('localhost',9091);
#my $transport = new Thrift::BufferedTransport($socket,1024,1024);
my $transport = new Thrift::FramedTransport($socket);
my $protocol  = new Thrift::BinaryProtocol($transport);
my $client    = new ThrudbClient($protocol);


eval{
    $transport->open();

    my $id = $client->store("Start");
    warn("saved $id");


    warn("Get: ".$client->fetch($id));

    $client->store("Update",$id);

    warn("Get: ".$client->fetch($id));

    $client->remove($id);

    warn("Removed $id");


}; if($@){
    warn(Dumper($@));
}

