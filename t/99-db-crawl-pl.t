#!perl

use strict;
use warnings;
use Test::More;
use File::Temp qw(tempdir);
use DBI;
use DBD::SQLite;
use POSIX qw(strftime);
use FindBin qw($Bin);

my $exec = "$Bin/../bin/db-crawl.pl";
die "Failed to find executable file at $exec, bailind out!"
    unless -f $exec;

my $ok;
my $dir = tempdir();
END {
    diag "See leftover files in $dir"
        unless $ok;
};

note "temp dir = $dir";

# prepare config
my $conf = "$dir/db-crawl.conf";
write_file( $conf, <<"CONF" );
    table tree id
    link2 tree.parent tree.id
CONF

# prepare database
my $ddl = sprintf q{
    -- created by %s on %s
    -- some stupid self-referencing table

    CREATE TABLE tree (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        node TEXT,
        parent INTEGER
    );
}, __FILE__, strftime("%Y-%m-%d %H:%M:%S", localtime);

my $content = q{
    INSERT INTO tree(id,node,parent) VALUES
    (1,'foo',NULL),
    (2,'bar',NULL),
    (3,'food',1),
    (4,'bard',2),
    (5,'bazooka',NULL),
    (6,'foobar',1),
    (7,'foodbard',3)
};

write_file( "$dir/setup.sql", "$ddl\n$content" );

my $dbfile = "$dir/data.sqlite";

my $dbh = DBI->connect( "dbi:SQLite:dbname=$dbfile", '', '', {RaiseError => 1} );

init_db( $dbh, $ddl );
init_db( $dbh, $content );

$ok = subtest "all" => sub {
    my ($code, $output) = read_cmd(
        perl       => $exec,
        '--config' => $conf,
        '--db'     => $dbfile,
        'tree:node=foo',
    );

    is $code, 0, "exit w/o problem";
    note $output;

    my @split = split /\s*;\s*/s, $output;

    like shift @split, qr/^\s*BEGIN/, "begin";
    like pop @split, qr/^COMMIT/, "commit";
    like $_, qr/INSERT.*'foo/, "foo-something inserted"
        for @split;
};

File::Temp::cleanup()
    if $ok;

done_testing;

# TODO copy-n-paste from t/06-* - maybe module?
sub init_db {
    my ($dbh, $ddl) = @_;

    foreach (split /;\n/, $ddl ) {
        /\S/ or next;
        $dbh->do($_);
    };

    return $dbh;
};

sub write_file {
    my ($fname, $content) = @_;

    open my $fd, ">", $fname
        or die "Failed to open(w) $fname: $!";
    print $fd $content
        or die "Failed to write to $fname: $!";
    close $fd
        or die "Failed to close(w) $fname: $!";

    return $fname;
};

# open2?
sub read_cmd {
    my (@cmd) = @_;

    note "running command: '@cmd'";

    my $pid = open(my $fd, "-|", @cmd)
        or die "Failed to run '@cmd': $!";

    local $/;
    defined(my $content = <$fd>)
        or die "Failed to read from '@cmd': $!";

    waitpid( $pid, 0 ) == $pid
        or die ("Wait for $pid failed after executing '@cmd'");

    my $signal = $? & 0xff;
    my $exit   = $? >> 8;
    return ($exit, $content);
};
