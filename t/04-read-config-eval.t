#!perl

use strict;
use warnings;
use Test::More;
use Test::Exception;

use DBIx::Crawl;

my $crawl = DBIx::Crawl->new;

my $conf = <<'CONF';
    table minimal id
    post_fetch minimal <<PERL
        my $data = shift;
        $data->{id} or die "no id found";
        $data->{name} = "Entry #$data->{id}";
    PERL
CONF

throws_ok {
    $crawl->read_config(make_fd($conf));
} qr/unsafe/, "No go until unsafe set";
note $@;

$crawl->unsafe(1);

lives_ok {
    $crawl->read_config(make_fd($conf));
} "unsafe set => ok";

my $hook = $crawl->_post_fetch_hooks->{minimal};
is ref $hook, 'CODE', "hooks created";

my $data = { id => 42 };
lives_ok {
    $hook->($data);
} "hook lives";
is_deeply $data, { id => 42, name => "Entry #42" }, "hook did what it claims to";

throws_ok {
    $hook->({});
} qr/no id found at <INPUT> line 4/, "error attributed correctly";
note $@;

done_testing;

sub make_fd {
    my ($str) = @_;

    open (my $fd, "<", \$str)
        or die "Failed to mmap: $!";

    return $fd;
};

