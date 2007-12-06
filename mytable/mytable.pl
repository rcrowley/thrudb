#!/usr/bin/perl

use strict;
use warnings;
use Data::Dumper;

my $size = 10;
my $master = new Master (split_size => $size, split_type => 'use');
print Dumper ($master);

$master->put (10, 44);
$master->put (-10, 42);
$master->put (-10, 43);
foreach (0..($size * 50))
{
    $master->put (int (rand ($size * 10)), rand);
    $master->put (int (-rand ($size * 10)), rand);
}
print Dumper ($master);

printf "%5s %5s %4s %20s %20s\n", 'gets', 'puts', 'size', 'start', 'end';
foreach my $data (@{$master->{datas}})
{
    printf "%5s %5s %4d %20d %20d\n", $data->{get_count}, $data->{put_count}, 
        $data->{size}, $data->{start}, $data->{end};
}

package Master;

use strict;
use warnings;
use Data::Dumper;

sub new
{
    my $class = shift @_;
    return bless {
        split_size => 10,
        datas => [new Data (
            start => -999999999999999999,
            end =>    999999999999999999,
        )],
        @_,
    }, $class;
}

sub get 
{
    my ($self, $key) = @_;
    return $self->_find ($key)->get ($key);
}

sub put
{
    my ($self, $key, $value) = @_;
    my $data = $self->_find ($key);
    my $ret = $data->put ($key, $value);
    if ($data->{size} + 1 > $self->{split_size})
    {
        $self->_split ($data);
    }
    return $ret;
}

sub _find
{
    my ($self, $key) = @_;
    foreach my $data (@{$self->{datas}})
    {
        if ($key <= $data->{end})
        {
            return $data;
        }
    }
    return undef;
}

sub _split
{
    my ($self, $data) = @_;
    my $index = 0;
    foreach my $d (@{$self->{datas}})
    {
        if ($d eq $data)
        {
            last;
        }
        $index++;
    }
    my $half;
    if ($self->{split_type} eq 'use')
    {
        $half = $data->{put_count} / 2.0;
        my $sum;
        foreach my $key (sort keys %{$data->{put_counts}})
        {
            $sum += $data->{put_counts}{$key};
            if ($sum >= $half)
            {
                $half = $key;
                last;
            }
        }
    }
    else # 'size'
    {
        $half = int ($data->{sum} / $data->{size});
    }
    my $d1 = new Data(start => $data->{start}, end => $half);
    my $d2 = new Data(start => $half, end => $data->{end});
    foreach my $key (keys %{$data->{data}})
    {
        if ($key <= $half)
        {
            $d1->put ($key, $data->{data}{$key});
        }
        else
        {
            $d2->put ($key, $data->{data}{$key});
        }
    }
    print Dumper ([$data, $half, $d1, $d2]);
    splice (@{$self->{datas}}, $index, 1, $d1, $d2);
}

package Data;

use strict;
use warnings;
use Data::Dumper;

sub new
{
    my $class = shift @_;
    return bless {
        size => 0,
        sum => 0,
        get_count => 0,
        put_count => 0,
        data => {},
        get_counts => {},
        put_counts => {},
        @_,
    }, $class;
}

sub get
{
    my ($self, $key) = @_;
    $self->{get_count}++;
    $self->{get_counts}{$key}++;
    return $self->{data}{$key};
}

sub put 
{
    my ($self, $key, $value) = @_;
    $self->{put_count}++;
    $self->{put_counts}{$key}++;
    unless (exists ($self->{data}{$key}))
    {
        $self->{sum} += $key;
        $self->{size}++ 
    }
    $self->{data}{$key} = $value;
}
