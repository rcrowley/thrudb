#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use Data::Dumper;
use DBI;

# create the database if not exist
# create the user if not exist
# create the directory if not exist
# make sure tablename doesn't already exist
# find the highest d_000...
# create the tablename d_000...'s inserting their data in to the directory

my $master_host = 'localhost';
my $master_port = 3306;
my $root_pass = 'mysqlpass';

my $database = 'diststore';
my $svc_hosts = '%';
my $user = 'diststore';
my $pass = 'pass';

my $tablename = 'data';
my $hosts = 'localhost:3306';
my $parts_per = 5;

GetOptions (
    'master_host=s' => \$master_host,
    'master_port=i' => \$master_port,
    'root_pass=s' => \$root_pass,
    'database=s' => \$database,
    'svc_hosts=s' => \$svc_hosts,
    'user=s' => \$user,
    'pass=s' => \$pass,
    'tablename=s' => \$tablename,
    'hosts=s' => \$hosts,
    'parts_per=s' => \$parts_per,
);

$hosts = [split (/,/, $hosts)];

print Dumper (
    {
    'master_host=s' => $master_host,
    'master_port=i' => $master_port,
    'root_pass=s' => $root_pass,
    'database=s' => $database,
    'svc_hosts=s' => $svc_hosts,
    'user=s' => $user,
    'pass=s' => $pass,
    'tablename=s' => $tablename,
    'hosts=s' => $hosts,
    'parts_per=s' => $parts_per,
    }
);

my $dsn = "DBI:mysql:database=mysql;host=$master_host;port=$master_port";
my $master_dbh = DBI->connect($dsn, 'root', $root_pass);

my @db_inits = (
    sprintf ("create database if not exists `%s`;", $database),
    sprintf ("grant select, update, insert, delete on `%s`.* to '%s'\@'%s' identified by '%s';",
        $database, $user, 'localhost', $pass),
    sprintf ("grant select, update, insert, delete on `%s`.* to '%s'\@'%s' identified by '%s';",
        $database, $user, $svc_hosts, $pass),
    sprintf ("use `%s`;", $database),
);

foreach (@db_inits)
{
    $master_dbh->do ($_) or die "do failed ($_): $@";
}

$master_dbh->do (sprintf "create table if not exists `directory` (
  `id` bigint(20) unsigned NOT NULL auto_increment,
  `tablename` char(32) NOT NULL,
  `start` double NOT NULL,
  `end` double NOT NULL,
  `host` varchar(128) NOT NULL,
  `port` smallint NOT NULL,
  `db` char(14) NOT NULL,
  `datatable` char(14) NOT NULL,
  `created_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  `retired_at` datetime default NULL,
  PRIMARY KEY  (`id`),
  UNIQUE KEY `UIDX_directory_tablename_datatable` (`tablename`,`datatable`),
  UNIQUE KEY `UIDX_directory_tablename_retired_at_end` (`tablename`,`retired_at`,`end`)
) ENGINE=innodb DEFAULT CHARSET=latin1;");

my $count = $master_dbh->selectrow_hashref (sprintf ("select count(datatable) as count from directory where `tablename` like '%s'", 
        $tablename))->{count};
die "tablename ($tablename) already exists\n" if ($count > 0);

my $dbnum = $master_dbh->selectrow_hashref ('select max(datatable) as max from directory where datatable like "d\_%"')->{max};
if ($dbnum)
{
    $dbnum =~ s/d_//;
    $dbnum++;
}
else
{
    $dbnum = 0;
}
printf "dbnum=%d\n", $dbnum;

my $range = 1.0 / (scalar (@$hosts) * $parts_per);

my $start = 0;
my $end = $range;
foreach my $host (@$hosts)
{
    printf "setting up $host\n";
    my $port = (split (/:/, $host))[1];
    $dsn = "DBI:mysql:database=$database;host=$host;port=$port";
    my $dbh = DBI->connect($dsn, 'root', $root_pass);

    foreach (@db_inits)
    {
        $dbh->do ($_) or die "do failed ($_): $@";
    }

    foreach my $db (1..$parts_per)
    {
        my $datatable = sprintf "d_%012d", $dbnum++;
        printf "\t$datatable\n";

        $end = $start + $range;

        my $cmd = sprintf ("insert into directory (tablename, start, end, host, db, datatable) values ('%s', '%04x', '%s', '%s', '%s', '%s')",
            $tablename, $start, $end, $host, $database, $datatable);
        $master_dbh->do ($cmd);

        $cmd = sprintf ("CREATE TABLE `%s` (
            `k` char(32) NOT NULL,
            `v` blob,
            `modified_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
            `created_at` timestamp NOT NULL default '0000-00-00 00:00:00',
            PRIMARY KEY  (`k`)
            ) ENGINE=innodb DEFAULT CHARSET=utf8 COLLATE=utf8_bin", $datatable);
        $dbh->do ($cmd);

        $start += $range;
    }
}
