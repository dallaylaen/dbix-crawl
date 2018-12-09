#!perl

use strict;
use warnings;
use Test::More;

use DBIx::Slice;

my $slice = DBIx::Slice->new;


$slice->add_table( book => 'id' );
$slice->add_table( author => 'id' );
$slice->add_table( critique => ["author_id", "book_id"] );

$slice->add_link( book => author_id => 'author' );

$slice->add_link( critique => author_id => 'author' => 'id' );
$slice->add_link( critique => book_id => 'book' );

# Now query this...


is_deeply (
    [ $slice->get_linked( critique => { author_id => 42, book_id => 137 } ) ],
    [ [ author => { id => 42 } ], [ book => { id => 137 } ], ],
    "Some basic querying works"
);


note explain $slice->make_primary_key( critique => { author_id => 42, book_id => 137 } );

done_testing;
