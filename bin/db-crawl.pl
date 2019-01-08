#!/usr/bin/env perl

use strict;
use warnings;
use DBI;
use Getopt::Long;
use File::Basename qw(dirname);

use lib dirname(__FILE__)."/../lib";
use DBIx::Crawl;

my %opt;
my %conn;
get_options_help (
    "$0 - dump partial database content based on links",
    "Usage: $0 [options] [table:field=value,...] ...",
    "Options may include",
    [ "config=s" => \$opt{config}, "(required) - list of known tables & links" ],
    [ "db=s"     => \$conn{dbi},   "- database to work on"],
    "        database is given as 'mysql:host=...;port=...'",
    "        if filename is given instead, assume SQLite",
    [ "user=s"   => \$conn{user},  "- database user" ],
    [ "pass=s"   => \$conn{pass},  "- database password" ], # TODO read online
    [ "help"     => \&display_usage, "- this message" ],
    "See `perldoc $0` for information about config file format",
);

die "--config is required"
    unless defined $opt{config};

# read config
my $fd = openfile( $opt{config} );
my $slice = DBIx::Crawl->new( unsafe => 1 );
$slice->read_config($fd);

# convert leftover args to Crawl's format
my @todo = map { arg_to_table($slice, $_) } @ARGV;

if (!@todo) {
    # Still SQL-compatible output
    print " -- nothing to be done\n";
    print " -- config file $opt{config} is OK\n";
    exit 0;
};

# assume SQLite to simplify testing
$conn{dbi} = "SQLite:dbname=$conn{dbi}"
    if defined $conn{dbi} and $conn{dbi} !~ /:/;

# fetch!
$slice->connect( %conn );
$slice->fetch( @todo );
print $slice->get_insert_script;

sub arg_to_table {
    # table:field="value",...
    my ($crawl, $arg) = @_;

    $arg =~ /^(\w+):((\w+=\w+)(,\w+=\w+)*|all)$/
        or die "Bad argument '$arg', must be 'table:field=value,...'\n";

    my $table = $1;
    my $spec  = $2;

    die "Unknown table '$table' requested by '$arg'\n"
        unless $crawl->_table_keys->{$table};

    # TODO detect duplicates & die
    my %hash;
    %hash = map { split /=/, $_, 2 } split /,/, $spec
        unless $spec eq 'all';

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

# TODO fix, replace, or finnd equivalent for Getopt::Helpful
# which does exactly the following but is unmaintained:

my @help;
sub get_options_help {
    my @list = @_;

    my @opt;

    foreach (@list) {
        $_ = [ $_ ] unless ref $_ eq 'ARRAY';
        push @help, $_;

        if (ref $_->[1]) {
            # found option
            push @opt, $_->[0], $_->[1];
        };
    };

    GetOptions( @opt )
        or die "Bad options. See $0 --help";
};

sub display_usage {
    my $status = shift;
    $status = 0 if defined $status and $status !~ /^\d+/;

    my @help_clear;

    foreach (@help) {
        if (ref $_->[1]) {
            my ($opt, $skip, @rest) = @$_;
            push @help_clear, join " ", option_to_help($opt), @rest;
        } else {
            push @help_clear, join " ", @$_;
        };
    };

    print join "\n", @help_clear, "";
    exit $status if defined $status;
};

sub option_to_help {
    return "  --".shift; # TODO
};

__END__

=head1 NAME

db-crawl.pl - fetch partial database content and print insert statements

=head1 USAGE

    db-crawl.pl [options] --config <config file> [table:field=value] ...

    Options may include
      --config=s (required) - list of known tables & links
      --db=s - database to work on
            database is given as 'mysql:host=...;port=...'
            if filename is given instead, assume SQLite
      --user=s - database user
      --pass=s - database password
      --help - this message

A config file is required for normal operation.

=head1 CONFIG FILE FORMAT

=head2 COMMENTS

Empty lines and lines starting with a pound (C<#>) are ignored.

=head2 COMMANDS

Each non-empty line must start with an alphanumeric B<command>,
followed by zero or more B<arguments>.

=head2 ARGUMENTS

An argument must be one of:

=over

=item * an unquoted string containing one or more of C<[A-Za-z0-9_.]>;

=item * a string in double quotes with backslash as escape character;

=item * a here-doc starting with C<E<lt>E<lt>> and a delimiter,
followed by zero or more lines and said delimiter again
surrounded by zero or more whitespace characters.
A delimiter may consist of one or more alphanumeric characters.

=back

=head2 COMMAND LIST

Commands are listed below,
each with a link to corresponding method in L<DBIx::Crawl>.

=head3 connect C<attribute> C<value>

Specify how database connection is made.

As an exception, the C<pass> attribute is prohibited.
This is done so because a configuration file is likely to be shared,
and the password better be shared through a separate channel.

See C<connect> and C<connect_info> attribute.

=head3 on_connect C<perl-code>

Perl code returning a sub to be executed upon connecting to database.

The sub must accept one argument, the database handle.

Strict and warnings are turned on for the code snippet,
and it is placed into a one-time separate package.

Setting C<unsafe> flag is required to make use of this command.

See C<post_connect_hook>.

=head3 table C<name> C<key-column>, ...

Add a table with corresponding key name(s).
All tables must be listed before any actions are performed on them.

=head3 link C<table1.field1> C<table2.field2>

Create a link between tables.

This may or may not correspond to actual foreign key in the database.

Each time a C<table1> item with nonempty C<field1> is fetched,
a query for ALL entries in C<table2> with the same C<field2> value
is queued.

See C<add_link>.

=head3 link2 C<table1.field1> C<table2.field2>

Like above, but the link is bidirectional.

See C<add_link_both>.

=head3 field_replace C<table.field> C<regexp> [C<replacement>]

If fetched value matches the regular expression, replace it with
replacement string (or NULL if not present).

C<$1>, C<$2> ... substitutes may be used.

See C<add_field_replace>.

=head3 post_fetch C<table> C<perl-code>

A sub to be executed whenever a row is fetched from table C<table>.

The first argument is the fetched row as hash reference.

See C<add_post_fetch>.

=head3 pre_insert_sql C<script>

SQL commands to be executed before insertion starts,
within the same transaction.

A command is expected to end in a semicolon.
No rigorous validation is made though.

See C<add_pre_insert_sql>.

=head3 post_insert_sql C<script>

SQL commands to be executed after insertion is finished,
within the same transaction.

See C<add_post_insert_sql>.

=head2 EXAMPLE

    # Specify database to connect to
    connect driver  mysql
    connect host    database.mycompany.com
    connect user    readonly
    # 'connect pass' is not allowed in config!

    # Some last-moment amendment
    on_connect <<PERL
        sub {
            my $dbh = shift;
            $dbh->do("SET NAMES utf8");
        };
    PERL

    # Add tables
    table artist id
    table album id

    # This one has a composite primary key
    #     which is usually a bad idea, but we can still handle it
    table song album_id track_number

    # Setup links
    link    album.artist_id     artist.id
    link2   album.id            song.album_id

    pre_insert_sql <<SQL
        SET NAMES utf8;
    SQL

=cut

