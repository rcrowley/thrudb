#!/usr/bin/perl

use strict;
use warnings;
use Getopt::Long;
use Data::Dumper;
use DBI;

# create the database if not exist
# create the user if not exist
# create the directory if not exist
# make sure bucket doesn't already exist
# find the highest d_000...
# create the bucket d_000...'s inserting their data in to the directory

my $master_host = 'localhost';
my $master_port = 3306;
my $root_pass = 'mysqlpass';

my $database = 'thrudoc';
my $svc_hosts = '%';
my $user = 'thrudoc';
my $pass = 'pass';

my $bucket = 'data';
my $hosts = 'localhost:3306';
my $with_circular_slaves = undef;
my $parts_per = 5;

GetOptions (
    'master-host=s' => \$master_host,
    'master-port=i' => \$master_port,
    'root-pass=s' => \$root_pass,
    'database=s' => \$database,
    'svc-hosts=s' => \$svc_hosts,
    'user=s' => \$user,
    'pass=s' => \$pass,
    'bucket=s' => \$bucket,
    'hosts=s' => \$hosts,
    'with-circular-slaves' => \$with_circular_slaves,
    'parts-per=s' => \$parts_per,
);

$hosts = [map { 
    my ($h, $p) = split /:/; 
    { hostname => $h, port => $p ? $p : 3306 } 
} split (/,/, $hosts)];

print Dumper (
    {
        'master-host=s' => $master_host,
        'master-port=i' => $master_port,
        'root-pass=s' => $root_pass,
        'database=s' => $database,
        'svc-hosts=s' => $svc_hosts,
        'user=s' => $user,
        'pass=s' => $pass,
        'bucket=s' => $bucket,
        'hosts=s' => $hosts,
        'with-circular-slaves' => $with_circular_slaves,
        'parts-per=s' => $parts_per,
    }
);

if ($with_circular_slaves and (scalar (@$hosts) % 2 != 0))
{
    die "error: --with-circular-slaves option passed, but odd number of hosts\n";
}

my $dsn = "DBI:mysql:database=mysql;host=$master_host;port=$master_port";
my $master_dbh = DBI->connect($dsn, 'root', $root_pass) 
    or die "unable to connect to master";

my @db_inits = (
    sprintf ("create database if not exists `%s`", $database),
    sprintf ("grant select, update, insert, delete on `%s`.* to '%s'\@'%s' identified by '%s'",
        $database, $user, 'localhost', $pass),
    sprintf ("grant select, update, insert, delete on `%s`.* to '%s'\@'%s' identified by '%s'",
        $database, $user, $svc_hosts, $pass),
    sprintf ("use `%s`", $database),
);

foreach (@db_inits)
{
    $master_dbh->do ($_) or die "do failed ($_): $@";
}

$master_dbh->do ("create table if not exists `host` (
  `id` bigint(20) unsigned NOT NULL auto_increment,
  `hostname` varchar(128) NOT NULL,
  `port` smallint NOT NULL,
  `slave_id` bigint(20) unsigned default NULL,
  PRIMARY KEY  (`id`),
  CONSTRAINT FOREIGN KEY `FK_host_slave_id` (`slave_id`) REFERENCES `host` (`id`)
) ENGINE=innodb DEFAULT CHARSET=latin1");

$master_dbh->do ("create table if not exists `directory` (
  `id` bigint(20) unsigned NOT NULL auto_increment,
  `bucket` char(32) NOT NULL,
  `start` double NOT NULL,
  `end` double NOT NULL,
  `host_id` bigint(20) unsigned NOT NULL,
  `db` char(14) NOT NULL,
  `datatable` char(14) NOT NULL,
  `created_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
  `retired_at` datetime default NULL,
  PRIMARY KEY  (`id`),
  UNIQUE KEY `UIDX_directory_bucket_datatable` (`bucket`,`datatable`),
  UNIQUE KEY `UIDX_directory_bucket_retired_at_end` (`bucket`,`retired_at`,`end`),
  CONSTRAINT FOREIGN KEY `FK_directory_host_id` (`host_id`) REFERENCES `host` (`id`)
) ENGINE=innodb DEFAULT CHARSET=latin1");

my $count = $master_dbh->selectrow_hashref (sprintf ("select count(`datatable`) as count from `directory` where `bucket` like '%s'", 
        $bucket))->{count};
die "bucket ($bucket) already exists\n" if ($count > 0);

my $dbnum = $master_dbh->selectrow_hashref ('select max(`datatable`) as max from `directory` where `datatable` like "d\_%"')->{max};
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
    printf ("setting up %s:%d\n", $host->{hostname}, $host->{port});
    $dsn = sprintf ("DBI:mysql:database=%s;host=%s;port=%d",
        $database, $host->{hostname}, $host->{port});
    my $dbh = DBI->connect($dsn, 'root', $root_pass)
        or die "unable to connect to host";

    foreach (@db_inits)
    {
        $dbh->do ($_) or die "do failed ($_): $@";
    }

    my $cmd = sprintf ("select id from `host` where `hostname` = '%s' and port = %d", 
        $host->{hostname}, $host->{port});
    $host->{id} = $master_dbh->selectrow_hashref ($cmd);
    if ($host->{id})
    {
        $host->{id} = $host->{id}->{id};
    }
    else
    {
        # create the host if it doesn't exist
        $master_dbh->do (sprintf ("insert into `host` (`hostname`, `port`) values ('%s', %d)",
                $host->{hostname}, $host->{port}));
        $host->{id} = $master_dbh->selectrow_hashref ($cmd)->{id};
    }

    foreach my $db (1..$parts_per)
    {
        my $datatable = sprintf "d_%012d", $dbnum++;
        printf "\t$datatable\n";

        $end = $start + $range;

        $cmd = sprintf ("insert into directory (`bucket`, `start`, `end`, `host_id`, `db`, `datatable`) values ('%s', %f, %f, %d, '%s', '%s')",
            $bucket, $start, $end, $host->{id}, $database, $datatable);
        $master_dbh->do ($cmd);

        $cmd = sprintf ("CREATE TABLE `%s` (
            `k` char(64) NOT NULL,
            `v` blob,
            `modified_at` timestamp NOT NULL default CURRENT_TIMESTAMP on update CURRENT_TIMESTAMP,
            `created_at` timestamp NOT NULL default '0000-00-00 00:00:00',
            PRIMARY KEY  (`k`)
            ) ENGINE=innodb DEFAULT CHARSET=utf8 COLLATE=utf8_bin", 
            $datatable);
        $dbh->do ($cmd);

        $start += $range;
    }
}

if ($with_circular_slaves)
{
    for (my $i = 0; $i < scalar (@$hosts); $i += 2)
    {
        printf "%s -> %s -> %s\n", 
            $hosts->[$i]->{hostname},
            $hosts->[$i+1]->{hostname},
            $hosts->[$i]->{hostname};
        my $sql = "update `host` set `slave_id`=%d where `id`=%d and slave_id is null";
        $master_dbh->do (sprintf ($sql, $hosts->[$i]{id}, $hosts->[$i+1]{id}));
        $master_dbh->do (sprintf ($sql, $hosts->[$i+1]{id}, $hosts->[$i]{id}));
    }
}
