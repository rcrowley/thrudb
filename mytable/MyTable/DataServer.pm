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
    #print STDERR "** new MyTable::DataServer ($class, @_)\n";
    my $self = bless {
        _db => new MyTable::DB (@_),
        put_counts => 0,
        get_counts => 0,
        increment_counts => 0,
        delete_counts => 0,
        @_,
    }, $class;
    my $tbl = $self->{tbl};
    $self->{_db}->prepare ($tbl, "put", "insert into $tbl (k, v) values (?, ?) on duplicate key update v = values(v)");
    $self->{_db}->prepare ($tbl, "get", "select v from $tbl where k = ?");
    $self->{_db}->prepare ($tbl, "increment", "insert into $tbl (k, v) values (?, ?) on duplicate key update v = v + values(v)");
    $self->{_db}->prepare ($tbl, "delete", "delete from $tbl where k = ?");
    $self->{_db}->prepare ($tbl, "walk_le", "select * from $tbl where k <= ? order by k asc");
    $self->{_db}->prepare ($tbl, "walk_gt", "select * from $tbl where k > ? order by k asc");

    $self->{_db}->do ("create table if not exists $tbl (k bigint not null primary key, v double not null)");

    $self->_update_size;

    return $self;
}

sub _update_size
{
    my ($self) = @_;

    my $tbl = $self->{tbl};
    $self->{size} = $self->{_db}->selectrow_hashref ("select count(k) size from $tbl")->{size};
    $self->{_db}->do ('update partitions set size = ? where id = ?', $self->{size}, $self->{id});
}

sub put
{
    my ($self, $key, $value) = @_;
    #print STDERR "** MyTable::DataServer->put ($self, $key, $value)\n";
    # TODO: verify that we're in the right data
    $self->{put_counts}++;
    return $self->{_db}->execute ($self->{tbl}, "put", $key, $value);
}

sub get
{
    my ($self, $key) = @_;
    #print STDERR "** MyTable::DataServer->get ($self, $key)\n";
    # TODO: verify that we're in the right data
    $self->{get_counts}++;
    $self->{_db}->execute ($self->{tbl}, "get", $key);
    my $row = $self->{_db}->fetchrow_hashref ($self->{tbl}, "get");
    return $row ? $row->{v} : undef;
}

sub increment
{
    my ($self, $key, $value) = @_;
    #print STDERR "** MyTable::DataServer->increment ($self, $key, $value)\n";
    # TODO: verify that we're in the right data
    $self->{increment_counts}++;
    return $self->{_db}->execute ($self->{tbl}, "increment", $key, $value);
}

sub delete
{
    my ($self, $key) = @_;
    #print STDERR "** MyTable::DataServer->delete ($self, $key)\n";
    # TODO: verify that we're in the right data
    $self->{delete_counts}++;
    return $self->{_db}->execute ($self->{tbl}, "delete", $key) or die "delete failed";
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
    $self->{_db}->execute ($self->{tbl}, 'walk_le', $half);
    foreach my $row (@{$self->{_db}->fetchall_arrayref ($self->{tbl}, 'walk_le')})
    {
        $d1->put (@$row);
    }
    $self->{_db}->execute ($self->{tbl}, 'walk_gt', $half);
    foreach my $row (@{$self->{_db}->fetchall_arrayref ($self->{tbl}, 'walk_gt')})
    {
        $d2->put (@$row);
    }
}

1;
