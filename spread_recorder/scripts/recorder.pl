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

# general recorder
$conn->subscribe (undef, 'channel', 1, sub { 
        my ($conn, $sender, $group, $mess_type, $message) = @_;
        db_put (db_for_group ($group), undef, message_to_key ($message),
            $message);
        printf "rec: %s - %s - %s\n", $group, $sender, $message;

        1;
    });

# catch me up
$conn->subscribe (undef, 'channel', 101, sub { 
        my ($conn, $sender, $group, $mess_type, $message) = @_;
        printf "catch: %s - %s - %s\n", $group, $sender, $message;

        # find the "next" message
        my $cur = db_for_group ($group)->cursor (undef, BDB::READ_UNCOMMITTED);
        my $data;
        # find the one they passed in
        db_c_get ($cur, message_to_key ($message), $data, BDB::SET_RANGE);
        # TODO: error handling
        # and move to the next
        db_c_get ($cur, message_to_key ($message), $data, BDB::NEXT);
        my $resp = "none";
        if ($!)
        {
            warn $!."\n";
        }
        elsif (defined $data)
        {
            $resp = $data;
        }
        printf "resp: %s - %s\n", $sender, $resp;
        $conn->queue ($sender, 101, $resp);
        db_c_close ($cur);

        1;
    });

$conn->run;

sub message_to_key 
{
    $_[0] =~ /^(\d+\.\d+)/;
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
