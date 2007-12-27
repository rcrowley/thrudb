# -*-perl-*-

use strict;
use warnings;
use Data::Dumper;
use Test::More;

use Thrift;
use Thrift::Socket;
use Thrift::FramedTransport;
use Thrift::BinaryProtocol;
use MyTable;

my $tests_left = 7;
plan tests => $tests_left;

eval 
{
    my $socket = new Thrift::Socket ("localhost", 9091);
    my $transport = new Thrift::FramedTransport ($socket);
    my $protocol = new Thrift::BinaryProtocol ($transport);
    my $client = new MyTableClient ($protocol);
    $transport->open;

    my $table = 'partitions';
    my $rand = rand;
    my $key = "key.$rand";
    my $value = "value.$rand";

    eval
    {
        $client->get ('nonexistent_table', 'nonexistent key');
    };
    ok ($@->{what} =~ /not found in directory/, 'nonexistent table');
    $tests_left--;

    eval
    {
        $client->get ($table, 'nonexistent key');
    };
    ok ($@->{what} =~ /not found/, 'nonexistent key');
    $tests_left--;

    $client->put ($table, $key, $value);
    is ($client->get ($table, $key), $value, 'put/get');
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
        $batch = $client->scan ("partitions", $batch->{seed}, $batch_size);
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
    ok ($@->{what} =~ /not found/, 'removed');
    $tests_left--;
};
if ($@)
{
    SKIP: {
        skip 'previous exception', --$tests_left;
    }
    fail ('exception thrown: '.
        UNIVERSAL::isa($@,"Thrift::TException") ? Dumper ($@) : $@
    );
}
