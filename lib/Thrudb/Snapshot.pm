#
# Copyright (c) 2007- T Jake Luciani
# Distributed under the New BSD Software License
#
# See accompanying file LICENSE or visit the Thrudb site at:
# http://thrudb.googlecode.com
#
#
package Thrudb::Snapshot;

use strict;
use warnings;

require 5.6.0;

sub new
{
    my $classname = shift;
    my $self      = {};

    my $c         = shift;

    $self->{c}    = $c;

    return bless( $self, $classname );
}

sub recover
{
    die "abstract";
}


sub create
{
    die "abstract";
}

sub uptodate
{
    die "abstract";
}

1;
