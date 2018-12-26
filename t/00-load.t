#!perl
use 5.006;
use strict;
use warnings;
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'DBIx::Crawl' ) || print "Bail out!\n";
}

diag( "Testing DBIx::Crawl $DBIx::Crawl::VERSION, Perl $], $^X" );
