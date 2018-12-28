#!perl

use strict;
use warnings;
use Test::More;
use Test::Exception;

use DBIx::Crawl;

my $crawl = DBIx::Crawl->new;

my $conf = <<'CONF';
    table minimal id
    post_fetch minimal
        my $data = shift;
        $data->{name} = "Entry #$data->{id}";
    __END__
CONF

throws_ok {
    $crawl->read_config(make_fd($conf));
} qr/unsafe/, "No go until unsafe set";
note $@;

$crawl->unsafe(1);

lives_ok {
    $crawl->read_config(make_fd($conf));
} "unsafe set => ok";

my $hook = $crawl->post_fetch_hooks->{minimal};
is ref $hook, 'CODE', "hooks created";

my $data = { id => 42 };
lives_ok {
    $hook->($data);
} "hook lives";
is_deeply $data, { id => 42, name => "Entry #42" }, "hook did what it claims to";

done_testing;

sub make_fd {
    my ($str) = @_;

    open (my $fd, "<", \$str)
        or die "Failed to mmap: $!";

    return $fd;
};

