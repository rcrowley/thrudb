#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;

use Spread::Connection;

my $conn = new Spread::Connection ('reader');

# listener
my $last_message;
my %messages;
$conn->subscribe (undef, 'channel', 1, sub { 
        my ($conn, $sender, $group, $mess_type, $message) = @_;
        printf "rec: %s - %s - %s\n", $group, $sender, $message;
        $messages{$group}{$message}++;
        $last_message = $message;

        1;
    });

#$conn->subscribe (undef, 'channel', undef, sub { 
#        my ($conn, $sender, $group, $mess_type, $message) = @_;
#        printf "dump: %s - %s - %s\n", $group, $sender, $message;
#        1;
#    });

my $replay_from;
my $first_back;

# catch me up
my %in_progress;
$conn->subscribe (undef, $conn->{private_group}, 101, sub { 
        my ($conn, $sender, $group, $mess_type, $message) = @_;
        printf "catch: %s - %s - %s\n", $group, $sender, $message;
        if ($message eq $first_back)
        {
            # we're done, just don't ask for more
        }
        else
        {
            # store the catch-up message
            $messages{'channel'}{$message}++;
            # ask for the next
            $conn->queue ('channel', 101, $message);
        }

        1;
    });

# install a handler that dumps the message on exit
$SIG{INT} = sub {
    $Data::Dumper::Sortkeys = 1;
    print Dumper (\%messages);
    exit 0;
};


# receive 5 messages
$conn->run (5);
$replay_from = $last_message;

# sleep for a bit, going around things to leave and then rejoin us
$conn->_leave ('channel');
sleep (5);
$conn->_join ('channel');

# run 1 interation to get the first back
$conn->run (1);
$first_back = $last_message;

printf "looking for stuff between %s and %s\n", $replay_from, $first_back;
# ask to be caught up
$conn->queue ('channel', 101, $replay_from);

# run forever
$conn->run;

__END__

# have list of received, have last received
# start listening
# record first back
# send last received
# start reading catchup
# stop when you you see first back
