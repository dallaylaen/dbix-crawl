#!perl -T
use 5.006;
use strict;
use warnings;
use Test::More;

plan tests => 1;

BEGIN {
    use_ok( 'DBIx::Slice' ) || print "Bail out!\n";
}

diag( "Testing DBIx::Slice $DBIx::Slice::VERSION, Perl $], $^X" );
