#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;

use Spread::Connection;

my $conn = new Spread::Connection ('recorder');

# general recorder
my %messages;
$conn->subscribe (undef, 'channel', 1, sub { 
        my ($conn, $sender, $group, $mess_type, $message) = @_;
        push @{$messages{$group}}, $message;
        printf "rec: %s - %s - %d - %s\n", $group, $sender, 
            scalar (@{$messages{$group}}), $message;

        1;
    });

# catch me up
$conn->subscribe (undef, 'channel', 101, sub { 
        my ($conn, $sender, $group, $mess_type, $message) = @_;
        printf "catch: %s - %s - %s\n", $group, $sender, $message;

        # find the "next" message
        # TODO: a binary search or something like that... anything but this
        my $group_mesg = $messages{$group};
        my $resp = 'none';
        if (defined $group_mesg)
        {
            for (my $i = 0; $i < scalar (@$group_mesg); $i++)
            {
                if ($message eq $group_mesg->[$i])
                {
                    $resp = $group_mesg->[$i+1];
                    # we've found the last, send back the next
                    last;
                }
            }
        }
        printf "resp: %s - %s\n", $sender, $resp;
        $conn->queue ($sender, 101, $resp);

        # we didn't find their last :(

        1;
    });

$conn->run;
