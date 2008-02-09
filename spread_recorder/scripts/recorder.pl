#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use BDB;
use Spread::Connection;

my $conn = new Spread::Connection ('recorder');

my $env = db_env_create;
my $db_dir = 'recorded';
mkdir $db_dir unless (-d $db_dir);

db_env_open ($env, $db_dir, 
    BDB::CREATE     | 
    BDB::RECOVER    |
    BDB::INIT_LOCK  |
    BDB::INIT_LOG   |
    BDB::INIT_MPOOL |
    BDB::INIT_TXN   |
    BDB::PRIVATE, 0600);

$env->set_flags (
    BDB::AUTO_COMMIT |
    BDB::CREATE, 1);

my %dbs;
# TODO: this needs to read in the max of lookup and start ++
my $logical_clock = 0;

# general recorder
$conn->subscribe (undef, 'thrudoc', 1, sub { 
        my ($conn, $sender, $group, $mess_type, $message) = @_;
        # get the uuid
        my $uuid = parse_uuid ($message);
        # store the uuid to logical_clock
        my $packed_clock = pack ('I', $logical_clock);
        db_put (db_for_group ($group . '.lookup'), undef, $uuid, 
            $packed_clock);
        # store the logical_clock to message
        db_put (db_for_group ($group), undef, $packed_clock,
            sprintf ("%s;%d;%s", $sender, $mess_type, $message));
        printf "store: %d - %s\n", $logical_clock, substr ($message, 0, 50);
        $logical_clock++;
        1;
    });

# catch me up
$conn->subscribe (undef, 'thrudoc', 101, sub { 
        my ($conn, $sender, $group, $mess_type, $message) = @_;
        printf "catch: %s - %s - %s\n", $group, $sender, 
            substr ($message, 0, 40);

        my $resp;

        # message is the last message the client received
        my $uuid = parse_uuid ($message);
        unless ($uuid)
        {
            $resp = ";;none";
            goto done;
        }

        # lookup the logical_clock
        my $clock;
        db_get (db_for_group ($group . '.lookup'), undef, $uuid, 
            $clock);
        if ($! == BDB::NOTFOUND)
        {
            $resp = ";;none";
            goto done;
        }

        # find the "next" message
        my $cur = db_for_group ($group)->cursor (undef, BDB::READ_UNCOMMITTED);
        # find the one they passed in
        $resp = '';
        my $key = $clock;
        db_c_get ($cur, $key, $resp, BDB::SET_RANGE);
        if ($! == BDB::NOTFOUND)
        {
            $resp = ";;none";
            goto done;
        }
        # and move to the next
        db_c_get ($cur, $key, $resp, BDB::NEXT);
        if ($! == BDB::NOTFOUND)
        {
            $resp = ";;none";
            goto done;
        }

done:
        printf "resp: %s - %d - %s\n", $sender, unpack ("I", $clock),
            substr ($resp, 0, 50);
        $conn->queue ($sender, 101, $resp);
        db_c_close ($cur);

        1;
    });

$conn->run;

sub parse_uuid
{
    $_[0] =~ /^([\w\-]+)/;
    return $1;
}

sub db_for_group
{
    my ($group) = @_;
    my $db = $dbs{$group};
    unless ($db)
    {
        $db = db_create $env;
        db_open ($db, undef, $group, undef, BDB::BTREE, 
            BDB::AUTO_COMMIT | 
            BDB::CREATE      |
            BDB::READ_UNCOMMITTED, 0600);
        $dbs{$group} = $db;
    }
    return $db;
}
