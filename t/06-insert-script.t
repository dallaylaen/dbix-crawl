#!perl

use strict;
use warnings;
use Test::More;
use Test::Exception;
use DBI;
use DBD::SQLite;

use DBIx::Crawl;

# Test::NoWarnings by hand. Just use it?
my @warn;
$SIG{__WARN__} = sub {
    my $msg = shift;
    push @warn, $msg;
    warn $msg;
};

my $dbfile = ":memory:";
my $dbh_in = DBI->connect("dbi:SQLite:dbname=$dbfile", '', '', { RaiseError => 1 });
my $ddl = <<"DDL";
    CREATE TABLE foo(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        parent INTEGER
    );
DDL
init_db($dbh_in, $ddl);
init_db($dbh_in, <<"INSERT");
    INSERT INTO foo(id,name,parent) VALUES
    (1, 'foo', NULL),
    (2, 'bar', NULL),
    (3, 'quux', 1),
    (4, 'foo''d', 1),
    (5, 'bard', 2),
    (6, 'foodbard', 4);
INSERT

# actual testing begins
my $crawl = DBIx::Crawl->new;

$crawl->read_config(\<<"CONF");
table foo id
link foo.parent foo.id
CONF

$crawl->connect( dbh => $dbh_in );
$crawl->fetch( [ foo => { name => 'foodbard' } ] );

my $partial = $crawl->get_insert_script;

subtest "partial dataset insert script" => sub {
    note $partial;

    my @parts = split(/\s*;\s*/s, $partial);
    is scalar @parts, 5, "5 stm issued";
    like $parts[0], qr/^\s*BEGIN/, "begin";
    like $parts[1], qr/^INSERT INTO\W+foo/, "insert statement";
    like $parts[2], qr/^INSERT INTO\W+foo/, "insert statement";
    like $parts[3], qr/^INSERT INTO\W+foo/, "insert statement";
    like $parts[4], qr/^COMMIT$/, "commit";
};

my $dbh_out = DBI->connect("dbi:SQLite:dbname=:memory:", '', '', { RaiseError => 1 });

init_db( $dbh_out, $ddl );

lives_ok {
    init_db( $dbh_out, $partial );
} "generated script actually works";

my $select = $dbh_out->prepare( "SELECT * FROM foo ORDER BY id" );
$select->execute;
my $data = $select->fetchall_arrayref({});

is_deeply $data, [
    {id => 1, name => "foo", parent => undef },
    {id => 4, name => "foo'd", parent => 1 },
    {id => 6, name => "foodbard", parent => 4 },
], "data as expected";

ok !@warn, "no warnings total";
done_testing;

sub init_db {
    my ($dbh, $ddl) = @_;

    foreach (split /;\n/, $ddl ) {
        /\S/ or next;
        $dbh->do($_);
    };

    return $dbh;
};
