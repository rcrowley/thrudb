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

use Thrudoc;

my $socket    = new Thrift::Socket('localhost',9091);
my $transport = new Thrift::FramedTransport($socket);
my $protocol  = new Thrift::BinaryProtocol($transport);
my $client    = new ThrudocClient($protocol);


eval{
    $transport->open();

    #warn(Dumper($client->fetchIds(0,100)));
    #return;

    #my $id = $client->store("Start","S3Test");
    #warn("saved $id");


    warn("Get: ".$client->fetch("S3Test"));

    #$client->store("Update",$id);

    #warn("Get: ".$client->fetch($id));

    #$client->remove($id);

    #warn("Removed $id");


}; if($@){
    warn(Dumper($@));
}

