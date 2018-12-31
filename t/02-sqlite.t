#!perl

use strict;
use warnings;
use Test::More;
use Test::Exception;
use DBI;
use DBD::SQLite;
use Log::Any::Test;
use Log::Any qw($log);

use DBIx::Crawl;

my $ddl = <<"SQL";
    CREATE TABLE good(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        price DECIMAL
    );
    CREATE TABLE manager (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    );
    CREATE TABLE customer (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL
    );
    CREATE TABLE receipt (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        manager_id INTEGER NOT NULL,
        customer_id INTEGER NOT NULL
    );
    CREATE TABLE receipt_good (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        receipt_id INTEGER NOT NULL,
        position INTEGER NOT NULL,
        good_id INTEGER NOT NULL,
        quantity INTEGER NOT NULL
    );
SQL

my $insert = <<"SQL";
    INSERT INTO manager(name) VALUES ('peter'),('paul'),('mary');
    INSERT INTO customer(name) VALUES ('ivan'),('taras'),('mykola');
    INSERT INTO good(name,price) VALUES ('cheese',56.3),('beef',25.6);

    INSERT INTO receipt(manager_id,customer_id) VALUES (1,1),(1,1),(1,2),(2,1),(2,3);
    INSERT INTO receipt_good
        (receipt_id,position,good_id,quantity)
    VALUES
        (1, 1, 1, 1),
        (1, 2, 1, 2),
        (1, 2, 1, 3),
        (2, 1, 1, 4),
        (3, 1, 1, 5),
        (4, 1, 2, 6),
        (5, 1, 2, 7)
    ;
SQL

my $dbh_in = DBI->connect( "dbi:SQLite:dbname=:memory:", '', '', {RaiseError => 1} );
my $dbh_out = DBI->connect( "dbi:SQLite:dbname=:memory:", '', '', {RaiseError => 1} );

foreach (split /;\n/s, $ddl) {
    note "$_";
    $dbh_in->do($_);
    $dbh_out->do($_);
};

foreach (split /;\n/s, $insert) {
    note "$_";
    $dbh_in->do($_);
};

my $slice = DBIx::Crawl->new;

foreach ( qw(good manager customer receipt )) {
    $slice->add_table( $_ => 'id' );
};
$slice->add_table( receipt_good => 'receipt_id', 'position' );

$slice->add_link( qw( manager id receipt manager_id ) );
$slice->add_link( qw( receipt manager_id manager id ) );
$slice->add_link( qw( receipt customer_id customer id ) );
$slice->add_link( qw( receipt id receipt_good receipt_id ) );
$slice->add_link( qw( receipt_good good_id good id ) );

$slice->connect( dbh => $dbh_in );
$slice->fetch( [ manager => { id => 1 } ] );

note explain $slice;

$slice->connect( dbh => $dbh_out );
throws_ok {
    $slice->insert;
} qr/read-only/, "nope! need rw=>1";
$slice->connect( dbh => $dbh_out, rw => 1 );
$slice->insert;

is_deeply( dump_table( $dbh_out, "manager", "id" ), [
    { id => 1, name => 'peter' }
], "manager imported partially" );

is_deeply( dump_table( $dbh_out, "customer", "id" ), [
    { id => 1, name => 'ivan' },
    { id => 2, name => 'taras' },
], "customer imported partially" );

done_testing;

sub dump_table {
    my ($dbh, $table, $order) = @_;

    my $sth = $dbh->prepare( "SELECT * FROM $table ORDER BY $order" );
    $sth->execute;
    return $sth->fetchall_arrayref({});
};
