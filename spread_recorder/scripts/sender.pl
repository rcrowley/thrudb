#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use Time::HiRes qw/time/;

use Spread::Connection;

my $conn = new Spread::Connection ('sender');

while (1)
{
    $conn->queue ('channel', 1, time);
    # -1 b/c we're not listening for anything and just want to dispatch
    # any waiting messages
    $conn->run (-1);
    sleep (1);
}
