#
#
#

package MyTable::DataServer;

use strict;
use warnings;
use Data::Dumper;

use MyTable::DB;

sub new
{
    my $class = shift @_;
    #print STDERR "** MyTable::DataServer->new ($class, @_)\n";
    my $self = bless {
        _db => MyTable::DB->new (@_),
        put_counts => 0,
        get_counts => 0,
        increment_counts => 0,
        delete_counts => 0,
        @_,
    }, $class;
    my $tbl = $self->{tbl};
    $self->{_db}->prepare ("put", "insert into $tbl (k, v) values (?, ?) on duplicate key update v = values(v)");
    $self->{_db}->prepare ("get", "select v from $tbl where k = ?");
    $self->{_db}->prepare ("increment", "insert into $tbl (k, v) values (?, ?) on duplicate key update v = v + values(v)");
    $self->{_db}->prepare ("delete", "delete from $tbl where k = ?");
    $self->{_db}->prepare ("walk", "select * from $tbl order by k asc");

    $self->{_db}->do ("create table if not exists $tbl (k bigint not null primary key, v double not null)");

    $self->{size} = $self->{_db}->selectrow_hashref ("select count(k) size from $tbl")->{size};

    return $self;
}

sub put
{
    my ($self, $key, $value) = @_;
    #print STDERR "** MyTable::DataServer->put ($self, $key, $value)\n";
    # TODO: verify that we're in the right data
    $self->{put_counts}++;
    return $self->{_db}->execute ("put", $key, $value);
}

sub get
{
    my ($self, $key) = @_;
    #print STDERR "** MyTable::DataServer->get ($self, $key)\n";
    # TODO: verify that we're in the right data
    $self->{get_counts}++;
    $self->{_db}->execute ("get", $key);
    my $row = $self->{_db}->fetchrow_hashref ("get");
    return $row ? $row->{v} : undef;
}

sub increment
{
    my ($self, $key, $value) = @_;
    #print STDERR "** MyTable::DataServer->increment ($self, $key, $value)\n";
    # TODO: verify that we're in the right data
    $self->{increment_counts}++;
    return $self->{_db}->execute ("increment", $key, $value);
}

sub delete
{
    my ($self, $key) = @_;
    #print STDERR "** MyTable::DataServer->delete ($self, $key)\n";
    # TODO: verify that we're in the right data
    $self->{delete_counts}++;
    return $self->{_db}->execute ("delete", $key) or die "delete failed";
}

sub key_average
{
    my ($self) = @_;
    #print STDERR "** MyTable::DataServer->key_average ($self)\n";
    my $tbl = $self->{tbl};
    return $self->{_db}->selectrow_hashref ("select avg(k) as v from $tbl")->{v};
}

sub split
{
    my ($self, $d1, $d2, $half) = @_;
    #print STDERR "** MyTable::DataServer->split ($self, $d1, $d2, $half)\n";
    $self->{_db}->execute ('walk');
    my $row;
    my $dt = $d1;
    while ($row = $self->{_db}->fetchrow_hashref ('walk'))
    {
        $dt = $d2 if ($row->{k} > $half);
        $dt->put ($row->{k}, $row->{v});
    }
}

1;
