#!/usr/bin/env perl

use strict;
use warnings;
use DBI;
use Getopt::Long;
use File::Basename qw(dirname);

use lib dirname(__FILE__)."/../lib";
use DBIx::Crawl;

my %opt;
GetOptions (
    "config=s" => \$opt{config},
    "db=s"     => \$opt{db},
    "user=s"   => \$opt{user},
    "pass=s"   => \$opt{pass}, # TODO read online
    "help"     => \&usage,
) or die "Bad usage";

die "--config is required"
    unless defined $opt{config};
die "--db is required"
    unless defined $opt{db};

$opt{db} = "SQLite:dbname=$opt{db}"
    if $opt{db} !~ /:/;

# parse args
my @todo;
foreach (@ARGV) {
    push @todo, arg_to_table($_);
};

# read config
my $fd = openfile( $opt{config} );
my $slice = DBIx::Crawl->new;
$slice->read_config($fd);

# connect to DB
my $dbh = DBI->connect( "dbi:$opt{db}", $opt{user}, $opt{pass}, { RaiseError => 1 } );

# fetch!
$slice->fetch( $dbh, @todo );
print $slice->get_insert_script;

sub arg_to_table {
    # table:field="value",...
    my ($arg) = @_;

    $arg =~ /^(\w+):((\w+=\w+)(,\w+=\w+)*)$/
        or die "Bad argument $arg, must be 'table:field=value,...'";

    my $table = $1;
    my $spec  = $2;
    # TODO detect duplicates & die
    my %hash = map { split /=/, $_, 2 } split /,/, $spec;

    return [ $table, \%hash ];
};

sub openfile {
    my ($name, $mode) = @_;

    $mode ||= '<';

    if ($name eq '-') {
        return $mode =~ />/ ? \*STDOUT : \*STDIN;
    };

    open my $fd, $mode, $name
        or die "Cannot open file($mode) '$name': $!";

    return $fd;
};
