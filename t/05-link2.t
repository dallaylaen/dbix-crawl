#!perl

use strict;
use warnings;
use Test::More;
use Test::Deep;

use DBIx::Crawl;

my $crawl = DBIx::Crawl->new;

$crawl->add_table( tree => "id" );
$crawl->add_link_both( tree => "id" => tree => "parent" );

cmp_deeply [$crawl->get_linked( tree => { id => 137, parent => 42 } ) ],
    bag( [ tree => { id => 42 } ], [ tree => { parent => 137 } ] ),
    "link worked both ways";


done_testing;
