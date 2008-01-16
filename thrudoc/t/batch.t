# -*-perl-*-

use strict;
use warnings;
use Data::Dumper;
use Test::More;

use Thrift;
use Thrift::Socket;
use Thrift::FramedTransport;
use Thrift::BinaryProtocol;
use DistStore;

my $tests_left = 35;
plan tests => $tests_left;

eval 
{
    my $socket = new Thrift::Socket ('localhost', 9091);
    my $transport = new Thrift::FramedTransport ($socket);
    my $protocol = new Thrift::BinaryProtocol ($transport);
    my $client = new DistStoreClient ($protocol);
    $transport->open;

    my $table = 'test.'.rand;
    my $rand = rand;
    my $key = "key.$rand";
    my $value = "value.$rand";

    # this should be a no-op if the table already exists, not all engines will
    # support this call, so it can't guarantee success
    unless ($client->admin ('create_tablename', $table) =~ /done/)
    {
        $table = 'test';
    }

    my $element = new DistStore::Element ({
            tablename => 'nonexistent_table',
            key => 'nonexistent key',
        });
    my $res = $client->getList ([$element]);
    isnt ($res->[0]{ex}{what}, '', 'nonexistent table');
    $tests_left--;

    $element->{tablename} = $table;
    $res = $client->getList ([$element]);
    isnt ($res->[0]{ex}{what}, '', 'nonexistent key');
    $tests_left--;

    my @elements;
    foreach (0..9)
    {
        $element = new DistStore::Element ({
                tablename => $table,
                key => 'key.'.$key,
                value => 'value.'.$value,
            });
        push @elements, $element;
    }

    $client->putList (\@elements);
    # these will have values, but they'll be ignored
    $res = $client->getList (\@elements); 
    is (scalar (@$res), scalar (@elements), 'put/get res size');
    $tests_left--;
    foreach (my $i = 0; $i < scalar (@elements); $i++)
    {
        is ($res->[$i]{element}{value}, $elements[$i]{value}, 'get/put '.$i); 
        $tests_left--;
    }

    # these will have values, but they'll be ignored
    $client->removeList (\@elements);
    $res = $client->getList (\@elements); 
    is (scalar (@$res), scalar (@elements), 'remove res size');
    $tests_left--;
    foreach (my $i = 0; $i < scalar (@elements); $i++)
    {
        isnt ($res->[$i]{ex}{what}, "", 'remove '.$i); 
        $tests_left--;
    }

    # these will have keys, but they'll be ignored
    $res = $client->putValueList (\@elements);
    is (scalar (@$res), scalar (@elements), 'putValue res size');
    foreach (my $i = 0; $i < scalar (@elements); $i++)
    {
        isnt ($res->[$i]{element}{key}, '', 'putValue '.$i);
        # replace the element's key with the gen'd one, so we can remove them 
        $elements[$i]{key} = $res->[$i]{element}{key};
    }
    # try and clean up after ourselves
    $client->removeList (\@elements);

    $client->admin ('delete_tablename', $table) if ($table ne 'test');
};
if ($@)
{
    SKIP: {
        skip 'previous exception', --$tests_left;
    }
    fail ('exception thrown: '.
        UNIVERSAL::isa($@,'Thrift::TException') ? Dumper ($@) : $@
    );
}

__DATA__
value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown value is way to long and will cause an exception to be thrown
