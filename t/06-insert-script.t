#!perl

use strict;
use warnings;
use Test::More;
use Test::Exception;

use FindBin qw($Bin);
use lib "$Bin/lib";
use Local::Test::Util;

use DBIx::Crawl;

# Test::NoWarnings by hand. Just use it?
my @warn;
$SIG{__WARN__} = sub {
    my $msg = shift;
    push @warn, $msg;
    warn $msg;
};

my $dbh_in = connect_sqlite;
my $ddl = <<"DDL";
    CREATE TABLE foo(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        parent INTEGER
    );
DDL
dbh_do($dbh_in, $ddl);
dbh_do($dbh_in, <<"INSERT");
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
pre_insert_sql <<SQL
    -- pre_insert; -- this is just a comment
SQL
post_insert_sql "-- this is just a post_insert comment;"
CONF

$crawl->connect( dbh => $dbh_in );
$crawl->fetch( [ foo => { name => 'foodbard' } ] );

my $partial = $crawl->get_insert_script;

subtest "partial dataset insert script" => sub {
    note $partial;

    my @parts = grep { /\S/ } split(/\n/s, $partial);
    list_like( \@parts, [
        qr/^\s*BEGIN/,
        qr/--.*pre_insert.*comment/,
        qr/^INSERT INTO\W+foo/,
        qr/^INSERT INTO\W+foo/,
        qr/^INSERT INTO\W+foo/,
        qr/--.*post_insert.*comment/,
        qr/^COMMIT;$/,
    ], "insert script as expected");
};

my $dbh_out = DBI->connect("dbi:SQLite:dbname=:memory:", '', '', { RaiseError => 1 });

dbh_do( $dbh_out, $ddl );

lives_ok {
    dbh_do( $dbh_out, $partial );
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


