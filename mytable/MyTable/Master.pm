#
#
#

package MyTable::Master;

use strict;
use warnings;
use Data::Dumper;

use MyTable::DB;
use MyTable::DataServer;

sub new
{
    my $class = shift @_;
    #print STDERR "** new MyTable::Master ($class, @_)\n";
    my $self = bless {
        _db => new MyTable::DB,
        split_size => 1000,
        @_,
    }, $class;
    
    # load the datas
    $self->_load;
    
    # prepare our finder statment
    $self->{_db}->prepare ('partitions', 'find', 'select id, end from partitions where ? <= end and retired_at is null order by end asc limit 1');
    $self->{_db}->prepare ('partitions', 'create', 'insert into partitions (start, end, host, db, tbl) values (?, ?, ?, ?, ?)');

    {
        # find out what our next table number will be
        my $max = $self->{_db}->selectrow_hashref ('select max(tbl) as val from partitions');
        $max->{val} =~ /_(\d+)/;
        $self->{next_tbl} = $1 + 1;
    }

    return $self;
}

sub _load
{
    my ($self) = @_;
    #print STDERR "** MyTable::Master->_load ($self)\n";
    foreach (@{$self->{_db}->selectall_arrayref ("select * from partitions where retired_at is null order by end asc")})
    {
        my ($id, $start, $end, $host, $db, $tbl) = @$_;
        $self->{datas}{$end} = new MyTable::DataServer (
            id => $id,
            start => $start,
            end => $end,
            host => $host,
            db => $db,
            tbl => $tbl,
        );
    }
}

sub balance
{
    my ($self) = @_;
    foreach my $data (values %{$self->{datas}})
    {
        if ($data->{size} > $self->{split_size})
        {
            $self->_split ($data);
        }
    }
}

# TODO: this needs to be mutex protected, and access to this data needs to be read-only/protected
sub _split
{
    my ($self, $data) = @_;
    #print STDERR "** MyTable::Master->_split ($self, $data)\n";

    my $half = int ($data->key_average + 0.5);

    my ($tbl, $id) = $self->_get_new_table ('localhost', 'mytable', $data->{start}, $half);
    my $d1 = new MyTable::DataServer(
        id => $id,
        host => 'localhost',
        db => 'mytable',
        tbl => $tbl,
        start => $data->{start}, 
        end => $half,
    );
    ($tbl, $id) = $self->_get_new_table ('localhost', 'mytable', $half, $data->{end});
    my $d2 = new MyTable::DataServer(
        id => $id,
        host => 'localhost',
        db => 'mytable',
        tbl => $tbl,
        start => $half,
        end => $data->{end}, 
    );

    # copy over data
    $data->split ($d1, $d2, $half);

    delete $self->{datas}{$data->{end}};
    $self->{datas}{$d1->{end}} = $d1;
    $self->{datas}{$d2->{end}} = $d2;

    $self->{_db}->do ('update partitions set retired_at = now() where id = ?', $data->{id});
    
}

sub _get_new_table
{
    my ($self, $host, $db, $start, $end) = @_;
    my $tbl = sprintf ('data_%08d', $self->{next_tbl}++);
    # TODO: host and db should be got at some other way
    $self->{_db}->execute ('partitions', 'create', $start, $end, $host, $db, $tbl);
    return ($tbl, $self->{_db}->last_insert_id);
}

sub put
{
    my ($self, $key, $value) = @_;
    #print STDERR "** MyTable::Master->put ($self, $key, $value)\n";
    my $ds = $self->_find ($key);
    return $ds->put ($key, $value);
}

sub get
{
    my ($self, $key) = @_;
    #print STDERR "** MyTable::Master->get ($self, $key)\n";
    my $ds = $self->_find ($key);
    return $ds->get ($key);
}

sub increment
{
    my ($self, $key, $value) = @_;
    #print STDERR "** MyTable::Master->increment ($self, $key, $value)\n";
    my $ds = $self->_find ($key);
    return $ds->increment ($key, $value);
}

sub delete
{
    my ($self, $key) = @_;
    #print STDERR "** MyTable::Master->delete ($self, $key)\n";
    my $ds = $self->_find ($key);
    return $ds->delete ($key);
}

sub _find
{
    my ($self, $key) = @_;
    #print STDERR "** MyTable::Master->_find ($self, $key)\n";
    $self->{_db}->execute ('partitions', 'find', $key);
    my $part = $self->{_db}->fetchrow_hashref ('partitions', "find");
    my $data = $self->{datas}{$part->{end}};
    die "a horrible death" if ($data->{id} ne $part->{id});
    return $data;
}

1;
