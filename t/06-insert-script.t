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
    is_multi_line( \@parts, [
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

# TODO use Assert::Refute for this
sub is_multi_line {
    my ($lines, $rex, $msg) = @_;

    $msg ||= "multiple lines match regexen";

    subtest $msg => sub {
        is scalar @$lines, scalar @$rex, "number of lines equals ".scalar @$rex;
        for( my $i = 0; $i<@$rex; $i++ ) {
            like $lines->[$i], $rex->[$i], "line $i matches $rex->[$i]";
        };
    };
};
