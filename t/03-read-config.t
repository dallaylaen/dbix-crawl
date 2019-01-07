#!perl

use strict;
use warnings;
use Test::More;
use Assert::Refute::T::Basic qw(is_deeply_diff);
use Test::Exception;

use DBIx::Crawl;

my $slice = DBIx::Crawl->new;

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
} qr/nknown command 'foobar'/, "unknown command = no go";

throws_ok {
    $slice->read_config(make_fd(<<CONF));
table aleph
CONF
} qr/wrong number of/, "arg number check (too few)";

throws_ok {
    $slice->read_config(make_fd(<<CONF));
table aleph id
link aleph 1 2 3 4
CONF
} qr/wrong number of.*<INPUT> line 2/, "arg number check (too many)";

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

link customer.id relation.customer_id
link relation.manager_id manager
CONF
} "normal config";

is_deeply_diff $slice->_table_keys,
    { customer => [ "id" ], manager => [ "id" ], relation => [ "customer_id", "manager_id" ] },
    10,
    "keys imported correctly";

note explain $slice->_table_links;

throws_ok {
    $slice->read_config (make_fd(<<'CONF'));
        post_fetch customer <<EOF
            my $data = shift;
            $data->{foobar} = 'hey';
        EOF
CONF
} qr/unsafe/, "unsafe command prohibited";

$slice->unsafe(1);

lives_ok {
    $slice->read_config (make_fd(<<'CONF'));
        post_fetch customer <<EOF
            my $data = shift;
            $data->{foobar} = 'hey';
        EOF
CONF
};

is ref $slice->post_fetch_hooks->{customer}, 'CODE', "A sub was inserted";

note explain $slice->post_fetch_hooks;

done_testing;

sub make_fd {
    my ($str) = @_;

    open (my $fd, "<", \$str)
        or die "Failed to mmap: $!";

    return $fd;
};
