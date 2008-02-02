# -*-perl-*-

use strict;
use warnings;
use Data::Dumper;
use Test::More tests => 21;

use Spread::Connection;

my $conn = new Spread::Connection ('tester');
ok ($conn, 'new Spread::Connection');
$conn->{see_self} = 1;

$conn->_join ('testing2');

my $all = 0;
$conn->subscribe (undef, undef, undef, sub {
        $all++;
        1;
    });

my $sender = 0;
$conn->subscribe ($conn->{private_group}, undef, undef, sub {
        $sender++;
        1;
    });

my $group = 0;
$conn->subscribe (undef, 'testing', undef, sub {
        $group++;
        1;
    });

my $type = 0;
$conn->subscribe (undef, undef, 42, sub {
        $type++;
        1;
    });

# test that returning 0 removes callback
my $once = 0;
$conn->subscribe (undef, undef, undef, sub {
        $once++;
        0;
    });

# everything
$conn->queue ('testing', 42, 'food');
$conn->run (1);
is ($all, 1, 'all');
is ($sender, 1, 'sender');
is ($group, 1, 'group');
is ($type, 1, 'type');
is ($once, 1, 'once');

# other type, once removed
$conn->queue ('testing', 43, 'food');
$conn->run (1);
is ($all, 2, 'all');
is ($sender, 2, 'sender');
is ($group, 2, 'group');
is ($type, 1, 'type');
is ($once, 1, 'once');

# other group
$conn->queue ('testing2', 42, 'food');
$conn->run (1);
is ($all, 3, 'all');
is ($sender, 3, 'sender');
is ($group, 2, 'group');
is ($type, 2, 'type');
is ($once, 1, 'once');

#  other sender...
my $other_conn = new Spread::Connection ('other');
$other_conn->queue ('testing', 42, 'food');
$other_conn->run (-1);
$conn->run (1);
is ($all, 4, 'all');
is ($sender, 3, 'sender');
is ($group, 3, 'group');
is ($type, 3, 'type');
is ($once, 1, 'once');
