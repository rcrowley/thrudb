# -*-perl-*-

use strict;
use warnings;
use Data::Dumper;
use Test::More;

use Thrift;
use Thrift::Socket;
use Thrift::FramedTransport;
use Thrift::BinaryProtocol;
use Thrudoc;

my $tests_left = 12;
plan tests => $tests_left;

eval 
{
    my $socket = new Thrift::Socket ('localhost', 9091);
    my $transport = new Thrift::FramedTransport ($socket);
    my $protocol = new Thrift::BinaryProtocol ($transport);
    my $client = new ThrudocClient ($protocol);
    $transport->open;

    my $table = 'test.'.rand;
    my $rand = rand;
    my $key = "key.$rand";
    my $value = "value.$rand";

    # this should be a no-op if the table already exists, not all engines will
    # support this call, so it can't guarantee success
    unless ($client->admin ('create_bucket', $table) =~ /done/)
    {
        $table = 'test';
    }

    eval
    {
        $client->get ('nonexistent_table', 'nonexistent key');
    };
    ok ($@->{what}, 'nonexistent table');
    $tests_left--;

    eval
    {
        $client->get ($table, 'nonexistent key');
    };
    ok ($@->{what}, 'nonexistent key');
    $tests_left--;

    eval
    {
        $client->get ('this_table_name_is_way_too_long_and_should_err_out',
            'key');
    };
    ok ($@->{what}, 'bucket length');
    $tests_left--;

    eval
    {
        $client->get ($table, 'key is way to long and will cause an exception to be thrown');
    };
    ok ($@->{what}, 'key length');
    $tests_left--;

    #eval
    #{
    #    $client->put ($table, 'key', <DATA>);
    #};
    #ok ($@->{what}, 'value length');
    #$tests_left--;

    $client->put ($table, $key, $value);
    is ($client->get ($table, $key), $value, 'put/get');
    $tests_left--;

    my $id;
    ok ($id = $client->putValue ($table, $value), 'putValue');
    is ($client->get ($table, $id), $value, 'putValue/get');
    $tests_left--;

    # a new value
    $value = rand;
    # write it twice
    $client->put ($table, $key, $value);
    is ($client->get ($table, $key), $value, 're-put/get');
    $tests_left--;

    # scan and make sure that we find what we put somewhere along the way
    my $batch_size = 20;
    my $batch;
    my $done = undef;
    do
    {
        $batch = $client->scan ($table, $batch->{seed}, $batch_size);
        foreach my $element (@{$batch->{elements}})
        {
            if ($element->{key} eq $key)
            {
                $done = $element->{value};
                last;
            }
        }
    } while (not $done and scalar (@{$batch->{elements}}) == $batch_size);
    ok ($done, 'scan found');
    $tests_left--;
    is ($done, $value, 'scan found correct');
    $tests_left--;

    $client->remove ($table, $key);
    eval
    {
        $client->get ($table, $key);
    };
    ok ($@->{what}, 'removed');
    $tests_left--;

    # just try and delete this one too
    $client->remove ($table, $id);

    ok (scalar (@{$client->getBuckets ()}) > 0, 'getBuckets');
    $tests_left--;

    $client->admin ('delete_bucket', $table) if ($table ne 'test');
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
