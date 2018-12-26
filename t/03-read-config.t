#!perl

use strict;
use warnings;
use Test::More;
use Test::Exception;

use DBIx::Slice;

my $slice = DBIx::Slice->new;

lives_ok {
    my $fd = make_fd( "foo\nbar\n" );
    my @all = <$fd>;
    is $all[0], "foo\n", "read 1 line";
    is $all[1], "bar\n", "read 2 line";
} "end self-test";

throws_ok {
    $slice->read_config(make_fd(<<CONF));
foobar aleph null
CONF
} qr/nknown command foobar/, "unknown command = no go";

throws_ok {
    $slice->read_config(make_fd(<<CONF));
table aleph
CONF
} qr/wrong number of/, "arg number check (too few)";

throws_ok {
    $slice->read_config(make_fd(<<CONF));
link aleph 1 2 3 4
CONF
} qr/wrong number of/, "arg number check (too many)";

throws_ok {
    $slice->read_config(make_fd(<<CONF));
table aleph id {"foo":123}
CONF
} qr/options not available/, "additional options recognized, but not supported";

lives_ok {
    $slice->read_config(make_fd(<<CONF));
# this is comment
table customer id
table manager id
table relation customer_id manager_id

link customer id relation customer_id
link relation manager_id manager id
CONF
} "normal config";

is_deeply $slice->keys,
    { customer => [ "id" ], manager => [ "id" ], relation => [ "customer_id", "manager_id" ] },
    "keys imported correctly";

note explain $slice->links;

done_testing;

sub make_fd {
    my ($str) = @_;

    open (my $fd, "<", \$str)
        or die "Failed to mmap: $!";

    return $fd;
};
