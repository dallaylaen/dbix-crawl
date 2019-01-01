#!perl

use strict;
use warnings;
use Test::More;

use FindBin qw($Bin);
use lib "$Bin/lib";
use Local::Test::Util;

use DBIx::Crawl;

my $dbh = connect_sqlite;
dbh_do( $dbh, <<SQL );
    CREATE TABLE user (
        id INTEGER,
        name VARCHAR(80),
        password VARCHAR(80)
    );
SQL
dbh_do( $dbh, <<SQL );
    INSERT INTO user(id,name,password) VALUES
        (1, 'peter', 'answer42'),
        (2, 'paul',  'answer137'),
        (3, 'crazy username with spaces', 'none');
SQL

my $crawl = DBIx::Crawl->new->connect( dbh => $dbh );

$crawl->read_config(\<<"CONF");
    table user id
    field_replace user.password "answer.*" "censored"
    field_replace user.name ".* .*"
CONF

$crawl->fetch( [ user => {} ] );

my $insert = $crawl->get_insert_script;

unlike $insert, qr/answer\d+/, "passwords censored";
like   $insert, qr/censored.*censored/s, "passwords censored (2)";

like   $insert, qr/\('?3'?, *NULL, *'none'\)/s, "bad username deleted";

done_testing;
