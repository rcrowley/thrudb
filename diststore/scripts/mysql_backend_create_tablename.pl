#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;
use DBI;

my $hostname = "localhost";
my $port = 3306;
my $database = "mytable2";

my $tablename = shift @ARGV || 'data';
my $part_per = shift @ARGV || 5;
my @hosts = scalar (@ARGV) ? @ARGV : ('localhost');

my $dsn = "DBI:mysql:database=$database;host=$hostname;port=$port";
my $master_dbh = DBI->connect($dsn, 'root', 'mysqlpass');

my $range = 65535;
my $part_range = $range / ($part_per * scalar (@hosts));
printf "%d -> %f -> %x\n", $range, $part_range, $part_range;

# TODO: the like probably won't be necessary long term, but still isn't a bad idea...
my $dbnum = $master_dbh->selectrow_hashref ('select max(datatable) as max from directory where datatable like "d\_%"')->{max};
$dbnum =~ s/d_//;
$dbnum++;

my $start = 0;
my $end;
foreach my $host (@hosts)
{
    printf "setting up $host\n";
    $dsn = "DBI:mysql:database=$database;host=$host;port=$port";
    my $dbh = DBI->connect($dsn, 'root', 'mysqlpass');
    foreach my $db (1..$part_per)
    {
        my $datatable = sprintf "d_%012d", $dbnum++;
        printf "\t$datatable\n";
        $end = $start + $part_range;
        $end = $end == $range ? 'ffffffffffffffffffffffffffffffff' : sprintf '%04x', $end;
        my $cmd = sprintf "insert into directory (tablename, start, end, host, db, datatable) values ('%s', '%04x', '%s', '%s', '%s', '%s')",
            $tablename, $start, $end, $host, $database, $datatable;
        $master_dbh->do ($cmd);

        $cmd = sprintf "CREATE TABLE `%s` (
    `k` char(37) NOT NULL,
    `v` blob,
    `modified_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
    `created_at` timestamp NOT NULL default '0000-00-00 00:00:00',
    PRIMARY KEY  (`k`)
) ENGINE=innodb DEFAULT CHARSET=utf8 COLLATE=utf8_bin", $datatable;
        $dbh->do ($cmd);

        $start += $part_range;
    }
}
