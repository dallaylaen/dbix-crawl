package
    Local::Test::Util;

use strict;
use warnings;
our $VERSION = 0.01;

=head1 NAME

Local::Test::Util - if you see this on CPAN, file a bug against DBIx::Crawl

=head1 DESCRIPTION

Utilitie functions to somplify testing L<DBIx::Crawl>.

All of these should probably be generalized & released as
standalone Test:: modules.

=cut

use parent qw(Exporter);
our @EXPORT = qw(connect_sqlite dbh_do list_like);

use Carp;
use DBI;
use DBD::SQLite;
use Test::More (); # we're not a test script

=head2 connect_sqlite( $db_name )

Returns an SQLite connection, either to a file or in-memory one.

=cut

sub connect_sqlite(;$) { ## no critic
    my $dbname = shift;

    $dbname ||= ":memory:";
    return DBI->connect( "dbi:SQLite:dbname=$dbname", '', '', { RaiseError => 1 } );
};

=head2 dbh_do( $handle, $statements )

Execute statements one by one.

This is needed because SQLite doesn't do() multiple statements quite well.

Statements are separated by semicolon+newline.

=cut

sub dbh_do($$) { ## no critic
    my ($dbh, $stm) = @_;

    my $n;
    eval {
        foreach (split /;\n/, $stm) {
            $n++;
            $dbh->do($_);
        };
        1;
    } || do {
        my $err = $@;
        chomp $err;
        croak "Statement $n failed: $err";
    };
};

=head2 list_like \@list, \@regexp, $message

=cut

sub list_like($$$) { ## no critic
    my ($lines, $rex, $msg) = @_;

    Test::More::subtest $msg => sub {
        Test::More::is scalar @$lines, scalar @$rex, "number of lines equals ".scalar @$rex;
        for( my $i = 0; $i<@$rex; $i++ ) {
            Test::More::like $lines->[$i], $rex->[$i], "line $i matches $rex->[$i]";
        };
    };
};

1;
