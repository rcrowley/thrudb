# -*-perl-*-

use strict;
use warnings;
use Data::Dumper;
use Test::More;

my $tests_left;
BEGIN { 
    $tests_left = 11;
    plan tests => $tests_left;

    use_ok ('Thrift');
    $tests_left--;
    use_ok ('Thrift::Socket');
    $tests_left--;
    use_ok ('Thrift::FramedTransport');
    $tests_left--;
    use_ok ('Thrift::BinaryProtocol');
    $tests_left--;
    use_ok ('DistStore');
    $tests_left--;
}

eval 
{
    ok (my $socket = new Thrift::Socket ("localhost", 9091), 'socket');
    $tests_left--;
    ok (my $transport = new Thrift::FramedTransport ($socket), 'transport');
    $tests_left--;
    ok (my $protocol = new Thrift::BinaryProtocol ($transport), 'protocol');
    $tests_left--;
    ok (my $client = new DistStoreClient ($protocol), 'client');
    $tests_left--;
    ok ($transport->open, 'transport->open');
    $tests_left--;
    is ($client->admin ('echo', 'data'), 'data', 'echo');
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
