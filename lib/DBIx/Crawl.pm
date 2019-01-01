package DBIx::Crawl;

use 5.010;
use strict;
use warnings;
our $VERSION = '0.01';

=head1 NAME

DBIx::Crawl - copy database content by references

=head1 VERSION

Version 0.01

=head1 SYNOPSIS

Quick summary of what the module does.

    use DBIx::Crawl;

    my $slice = DBIx::Crawl->new();

    # recreate DSL
    $slice->add_table (...);
    $slice->add_link ( ... );

    # fetch data
    $slice->fetch( $dbh_prod, [ foo => { id => 42 } ], [ bar => { id => 137 } ] );

    # store data
    $slice->insert( $dbh_test );

=head1 ATTRIBUTES

=over

=item C<unsafe> - allow unsafe operations, like execution of user-supplied code
(default: off)

=item C<keys> - table to primary key hash

=item C<links> - foreign key hash

=item C<data> - fetched data cache based on primary key

=item C<seen> - fetched data search cache to avoid looping or checking out data twice.

=item C<post_fetch_hooks> - per-table post-fetch processing, like removing passwords etc.

=item C<connect_info> - default values to connect to database

=item C<dbh> - database connection to be used for fetching/storing data.

=item C<dbh_allow_write> - unless set, all attempts to write to database will fail.

=item C<post_connect_hook> - execute this code on database handle upon connection.

=item C<pre_insert_sql> - command(s) to be executed on insert transaction start.

=item C<post_insert_sql> - command(s) to be executed on insert transaction end.

=back

=cut

use Carp;
use Log::Any qw($log);
use Moo;

# field => value, e.g. port, host, user ...
has connect_info => is => "rw", default => sub { {} };

has dbh => is => "rw";
has dbh_allow_write => is => "rw" => default => sub { 0 };
has post_connect_hook => is => "rw";

has pre_insert_sql    => is => "rw", default => sub { [] };
has post_insert_sql   => is => "rw", default => sub { [] };

# Allow execution of client code
has unsafe => is => "rw", default => sub { 0 };

# table => [ field, ... ]
has keys   => is => "rw", default => sub { {} };

# table => field => [ table : field, ... ]
has links  => is => "rw", default => sub { {} };

# table => [ {record}, ... ]
has data   => is => "rw", default => sub { {} };

# table => 'keypair' => 'valuepair'
has seen => is => "rw", default => sub { {} };

# table => sub { ... }
# TODO rename
has post_fetch_hooks => is => "rw", default => sub { {} };

# DDL

=head1 PUBLIC API

As of current, cache, DBI layer, and database structure class are all mixed
together.
This may or may not change in the future.

=head2 HIGH-LEVEL SETUP

=head3 connect( %options )

Connect to a database, or use existing connection.

Options may include:

=over

=back

=cut

sub connect {
    my ($self, %opt) = @_;

    # first and foremost, the special options
    my $rw  = delete $opt{rw};
    my $dbh = delete $opt{dbh};

    if (!$dbh) {
        # if we've read anything from config, assume it as default
        defined $opt{$_} or delete $opt{$_}
            for keys %opt;
        my $default = $self->connect_info;
        %opt = ( %$default, %opt );

        my $driver = delete $opt{driver};
        my $user   = delete $opt{user};
        my $pass   = delete $opt{pass};
        my $extra  = delete $opt{extra} || {};
        $extra = { RaiseError => 1, %$extra };

        my $dbi    = delete $opt{dbi};
        croak "don't know where to connect to - either dbh, dbi, or driver must be set"
            unless $dbi || $driver;

        $dbi     ||= "$driver:".join ";",
            map { "$_=$opt{$_}" } grep { defined $opt{$_} } keys %opt;

        $dbi = "dbi:$dbi" unless $dbi =~ /^dbi:/;

        require DBI;
        $dbh = DBI->connect($dbi, $user, $pass, $extra);

        if (my $code = $self->post_connect_hook) {
            $code->($dbh);
        };
    };

    $self->dbh( $dbh );
    $self->dbh_allow_write( $rw ? 1 : 0 );

    return $self;
};

=head2 DDL METHODS

The following methods are used to setup table structure before fetching
any data.

=head3 add_table

    add_table( table_name => key_field )

Add a table to known table list, add a primary key.

=cut

sub add_table {
    my ($self, $table, @key) = @_;

    $self->keys->{$table} = \@key;
};

=head3 add_link

    add_link( $table, $field, $linked_table, $linked_field

Create a link that suggests that for every item in $table,
corresponding item(s) in $linked_table must also be fetched.

Note that this doe not directly correspond to a foreign key in SQL.
For instance, if one has tables C<author> and C<book>,

    add_link( "book", "author_id", "author", "id" )

means that for every book, its author must be fetched, whereas

    add_link( "author", "id", "book", "author_id" )

means that for every author, ALL of their books are fetched.

=cut

sub add_link {
    my ($self, $table, $fk, $ref, $pk) = @_;

    croak "Cannot add link from unknown table '$table'"
        unless $self->keys->{$table};
    croak "Cannot add link to unknown table '$ref'"
        unless $self->keys->{$ref};

    $pk ||= $self->keys->{$ref}[0];

    $self->links->{$table}{$fk}{$ref.'.'.$pk}++;
    return $self;
};

=head3 add_link_both

Adds a bidirectional link.

=cut

sub add_link_both {
    my ($self, $table, $fk, $ref, $pk) = @_;

    $pk ||= $self->keys->{$ref}[0];

    $self->add_link( $table, $fk, $ref, $pk );
    $self->add_link( $ref, $pk, $table, $fk );
};

=head3 add_post_fetch

    add_post_fetch( my_table => \&_adjust )

    sub _adjust {
        my $data = shift;
        $data->{password} = "*******";
        ...
    };

Add a coderef to postprocess fetched items.

The item is given as a hashref.
CODE must act on that hashref directly.
Return value is ignored.

=cut

sub add_post_fetch {
    my ($self, $table, $code) = @_;

    $self->post_fetch_hooks->{$table} = $code;
};

=head3 add_pre_insert_sql

Add an SQL statement to be executed AFTER insert transaction starts
but BEFORE any actual insert is made.

Multiple statements may be added, and will be executed in order.

The statement must end in a semicolon and optional comment starting with a C<-->.

=cut

sub add_pre_insert_sql {
    my ($self, $sql) = @_;

    $sql =~ /;\s*(?:--[^\n]*)?\n*$/
        or croak "SQL must end in a semicolon(;) and optionally a comment";

    push @{ $self->pre_insert_sql }, $sql;
};

=head3 add_post_insert_sql

Add an SQL statement to be executed AFTER inserts are made
but BEFORE insert transaction ends.

Multiple statements may be added, and will be executed in order.

The statement must end in a semicolon and optional comment starting with a C<-->.

=cut

sub add_post_insert_sql {
    my ($self, $sql) = @_;

    # TODO need to parse actual SQL to prevent runaways
    # this at least protects from most stupid errors
    $sql =~ /;\s*(?:--[^\n]*)?\n*$/
        or croak "SQL must end in a semicolon(;) and optionally a comment";

    push @{ $self->post_insert_sql }, $sql;
};

=head3 read_config( $file_handle )

Read configuration file. Docs TBD.

=cut

my %command_spec = (
    connect => {
        method => sub {
            my ($self, $field, $value) = @_;
            $self->connect_info->{$field} = $value
        },
        min    => 2,
        max    => 2,
    },
    table => {
        method => "add_table",
        min    => 2,
    },
    link  => {
        method => "add_link",
        min    => 2,
        max    => 2,
        args   => \&_args_link,
    },
    link2 => {
        method => "add_link_both",
        min    => 2,
        max    => 2,
        args   => \&_args_link,
    },
    post_fetch => {
        method => 'add_post_fetch',
        min    => 2,
        max    => 2,
        unsafe => 1,
        args   => sub {
            my ($where, $table, $code) = @_;
            return ($table, _compile_hook($where, $code));
        },
    },
    on_connect => {
        method => 'post_connect_hook',
        min    => 1,
        max    => 1,
        unsafe => 1,
        args   => \&_compile_hook,
    },
    pre_insert_sql => {
        method => 'add_pre_insert_sql',
        min => 1,
        max => 1,
    },
    post_insert_sql => {
        method => 'add_post_insert_sql',
        min => 1,
        max => 1,
    },
);

my $re_arg = qr([\w\.]+|"(?:[^"]+|\\")*"|<<\w+);

sub read_config {
    my ($self, $fd, $fname) = @_;

    if (ref $fd eq 'SCALAR') {
        open my $fdcopy, "<", $fd
            or croak "Failed to mmap scalar $fd: $!";
        $fd = $fdcopy;
    };

    $fname ||= '<INPUT>';

    my $line; # global because we want to die at appropriate place
    eval {
        my @todo;
        my @raw_cmd = _tokenize_file($fd, \$line);

        foreach my $found (@raw_cmd) {
            ($line, my ($command, $extra, @args)) = @$found;

            my $spec = $command_spec{$command};

            die "unknown command '$command'"
                unless $spec;

            die "wrong number of arguments for '$command'"
                unless @args >= ($spec->{min} // 0) and @args <= ($spec->{max} // 9**9**9);

            die "command '$command' is unsafe, but unsafe mode not turned on"
                if $spec->{unsafe} and not $self->unsafe;


            @args = $spec->{args}->([$fname, $line], @args)
                if $spec->{args};

            push @todo, [ $line, $spec->{method}, @args ];
        };

        # ok, the file was read...
        foreach my $cmd (@todo) {
            ($line, my( $method, @rest )) = @$cmd;
            $self->$method( @rest );
        };
        1;
    } || do {
        my $err = $@;
        # die with config file line:number and not calling code
        $err =~ s/ +at .*? line \d+\.?\n?$//s;
        $err .= " in $fname line $line\n";
        die $err;
    };

    # all folks
    return $self;
};

# in:  $filehandle, $ref_to_line_number
# out: [ line, command, \%opts, @args ], ...
sub _tokenize_file {
    my ($fd, $line) = @_;

    my @out;
    while (<$fd>) {
        $$line++;
        # comment
        /\S/ or next;
        /^\s*#/ and next;

        /^\s*(\w+)((?:\s+$re_arg)*)(?:\s+(\{.*\}))?\s*$/
            or die "Bad line format: $_";

        my ($command, $allargs, $opt) = ($1, $2, $3);

        # TODO decode opt
        die "options not available for '$command'"
            if $opt;

        my @args;
        ARG: foreach ($allargs =~ /($re_arg)/g) {
            if (/^<<(\w+)$/) {
                # slurp part of file, adjust line
                my $eof = $1;
                my $rex = qr(\s*\Q$eof\E\s*$);
                my @parts;
                while (<$fd>) {
                    if ($_ =~ $rex) {
                        push @args, join '', @parts;
                        next ARG;
                    }
                    push @parts, $_;
                };
                die "runaway argument - could not find a $eof until end of file";
            } else {
                push @args, _unquote($_);
            };
        };

        push @out, [ $$line, $command, undef, @args ];
    };

    return @out;
};

my %unquote_replace = ( n => "\n" );
sub _unquote {
    my $str = shift;

    return $str if $str =~ /^[\w\.]+$/;

    if ( $str =~ s/^"// and $str =~ s/"$// ) {
        $str =~ s/\\(.)/$unquote_replace{$1} || $1/ge;
        return $str;
    };

    confess "Bug in ".__PACKAGE__.", cannot unquote line: '$str'";
};

my $pkg_id;
sub _compile_hook {
    my ($where, $content) = @_;

    my $package = __PACKAGE__."::__ANON__::".++$pkg_id;
    my ($file, $line) = @$where;
    my $coderef = eval ## no critic
    qq{
        package $package;
        use strict;
        use warnings;\n# line $line $file
        sub {\n$content };
    };
    croak "Compilation of user supplied code failed: $@"
        unless $coderef;

    return $coderef;
};

sub _args_link {
    my ($where, $from, $to) = @_;
    my @out;
    $from =~ /^(\w+)\.(\w+)$/
        or die ("First argument must be table.field for command 'link'");
    push @out, $1, $2;
    $to   =~ /^(\w+)(?:\.(\w+))?$/
        or die ("Second argument must be table.field or just table for command 'link'");
    push @out, $1, $2?$2:();
    return @out;
};

=head2 DDL QUERYING

The following functions apply known tables and links to data,
returning more data.
They are all pure i.e. only produce output that only depends on parameters.

=head3 make_key

    maky_key( \%hash )

Returns a pair of stringified values and keys of hash.

Current format is tab-delimited. DO NOT RELY.

=cut

sub make_key {
    my ($self, $data) = @_;

    my @keys = sort keys %$data;
    return ( $self->joinf( @keys ), $self->joinf( map { $data->{$_} } @keys ) );
};

=head3 make_primary_key

    make_primary_key( table, \%hash )

Create a key (stringified in the same way) based on a known table and some values.

=cut

sub make_primary_key {
    my ($self, $table, $data) = @_;

    my $keys = $self->keys->{$table};

    return ( $self->joinf( @$keys ), $self->joinf( map { $data->{$_} } @$keys ) );
};

=head3 joinf

Stringify arbitrary array.

=cut

# TODO configurable delimiter
sub joinf {
    my $self = shift;

    return join "\t", @_;
};

=head3 splitf

Inverse of joinf.

=cut

sub splitf {
    my ($self, $row) = @_;

    return split /\t/, $row;
};

=head3 get_linked

    get_linked( $table, \%data )

For every non-null value in hash, display links. The format is

    [ other_table => { key => value } ], ...

=cut

sub get_linked {
    my ($self, $table, $data) = @_;

    my @ret;
    my $links = $self->links->{$table};
    foreach my $field (sort keys %$links) {
        next unless defined $data->{$field};
        foreach my $pair (sort keys %{ $links->{$field} } ) {
            my ($ref, $fk) = split /\./, $pair, 2;
            push @ret, [ $ref => { $fk => $data->{$field} } ];
        };
    };

    return @ret;
};

=head2 CACHE PROCESSING METHODS

=head3 clear

Remove cache.

=cut

sub clear {
    my $self = shift;
    $self->data({});
    $self->seen({});
    return $self;
};

=head3 is_seen( table => \%key )

Check that table was ever queried for \%key

=cut

sub is_seen {
    my ($self, $table, $data) = @_;

    my ($key, $value) = $self->make_key( $data );

    return $self->seen->{$table}{$key}{$value};
};

=head3 mark_seen( table => \%key )

Mark a query as known. The previous value is returned.

=cut

sub mark_seen {
    my ($self, $table, $data) = @_;

    my ($key, $value) = $self->make_key( $data );

    return $self->seen->{$table}{$key}{$value}++;
};

=head3 add_data

    add_data( $table, \%data )

Add data to table content. If any content with the same primary key exists,
it is replace.

=cut

sub add_data {
    my ($self, $table, $data) = @_;

    my (undef, $key) = $self->make_primary_key( $table, $data );

    $self->data->{$table}{$key} = $data;
    return $self;
};

=head3 get_insert_script

Create a would-be insert transaction in plain SQL,
without applying it anywhere.

=cut

sub get_insert_script {
    my ($self, %opt) = @_;

    # TODO %opt unused

    my $all = $self->data;

    my @work;

    push @work, "BEGIN;";
    push @work, @{ $self->pre_insert_sql };
    foreach my $table( keys %$all ) {
        my $entries = $all->{$table};
        foreach my $item (values %$entries) {
            my @keys = sort keys %$item;
            my @esc_keys = map { "`$_`" } @keys;
            my @values = map { _value2sql($_) } @$item{@keys};
            push @work, sprintf "INSERT INTO `%s`(%s) VALUES (%s);",
                $table, (join ", ", @esc_keys), (join ", ", @values);
        };
    };
    push @work, @{ $self->post_insert_sql };
    push @work, "COMMIT;";
    return join "\n", @work, '';
};

sub _value2sql {
    my $str = shift;
    return 'NULL' unless defined $str;
    $str =~ s/'/''/g;
    return "'$str'";
};

=head2 DATABASE CONNECTION METHODS

=head3 fetch( @list )

Fetch data elements, recursively, and store them in cache.
Every data point is given as

    [ table_name => \%search_criteria ]

Links are followed, with aggressive caching.

=cut

sub fetch {
    my ($self, @todo) = @_;

    while (@todo) {
        my $req = shift @todo;
        my ($table, $key) = @$req;

        my $seen = $self->mark_seen( $table, $key );
        my @req = $self->make_key( $key );

        $log->info( "\tfetching from $table where ($req[0]) = ($req[1]), seen=$seen" );
        next if $seen;

        my @data = $self->fetch_one( $table, $key );

        $log->info("\tGot items: ", scalar @data);

        foreach (@data) {
            $self->add_data($table => $_);
            push @todo, $self->get_linked( $table => $_ );
        };
    };
};

=head3 fetch_one

Execute one query and return it, without modifying anything.

=cut

sub fetch_one {
    my ($self, $table, $key) = @_;

    my @args;
    my $sql = "SELECT * FROM `$table` WHERE 1";
    foreach (keys %$key) {
        # TODO if defined
        $sql .= " AND `$_` = ?";
        push @args, $key->{$_};
    };

    $log->info( "\tfetch: $sql [@args]" );

    my $sth = $self->dbh->prepare( $sql );
    $sth->execute(@args);

    my @ret;
    my $hook = $self->post_fetch_hooks->{$table};
    while (my $row = $sth->fetchrow_hashref) {
        $hook->($row) if $hook;
        push @ret, $row;
    };

    return @ret;
};

=head3 insert

Insert ALL cached data into target database as one transaction.

=cut

sub insert {
    my ($self) = @_;

    $self->_rw_check;
    my $data = $self->data;

    my @todo;
    foreach my $table( keys %$data ) {
        foreach my $item ( values %{$data->{$table}} ) {
            push @todo, [ $table, $item ];
        };
    };

    $self->dbh->begin_work;
    $self->dbh->do($_)
        for @{ $self->pre_insert_sql };
    foreach (@todo) {
        $self->insert_one( @$_ );
    };
    $self->dbh->do($_)
        for @{ $self->post_insert_sql };
    $self->dbh->commit;

};

=head3 insert_one

Insert a C<[ table => \%data ]> tuple into a database, without affecting cache
in any way.

=cut

sub insert_one {
    my ($self, $table, $data) = @_;

    $self->_rw_check;

    my @fields = sort grep { defined $data->{$_} } keys %$data;
    my $fields = join ",", map { "`$_`" } @fields;
    my $quest  = join ",", map { "?" } @fields;
    my @args   = map { $data->{$_} } @fields;

    my $sql = "INSERT INTO `$table`($fields) VALUES( $quest )";

    $log->info("\t$sql [@args]");

    my $sth = $self->dbh->prepare( $sql );
    $sth->execute( @args );
};

sub _rw_check {
    my $self = shift;

    croak "read-write operation requested in read-only mode, set rw=>1 on connect"
        unless $self->dbh_allow_write;

    $self;
};

=head1 AUTHOR

Konstantin S. Uvarin, C<< <khedin at gmail.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-dbix-slice at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DBIx-Crawl>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.


=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DBIx::Crawl


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=DBIx-Crawl>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/DBIx-Crawl>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/DBIx-Crawl>

=item * Search CPAN

L<http://search.cpan.org/dist/DBIx-Crawl/>

=back


=head1 ACKNOWLEDGEMENTS


=head1 LICENSE AND COPYRIGHT

Copyright 2018 Konstantin S. Uvarin.

This program is free software; you can redistribute it and/or modify it
under the terms of the the Artistic License (2.0). You may obtain a
copy of the full license at:

L<http://www.perlfoundation.org/artistic_license_2_0>

Any use, modification, and distribution of the Standard or Modified
Versions is governed by this Artistic License. By using, modifying or
distributing the Package, you accept this license. Do not use, modify,
or distribute the Package, if you do not accept this license.

If your Modified Version has been derived from a Modified Version made
by someone other than you, you are nevertheless required to ensure that
your Modified Version complies with the requirements of this license.

This license does not grant you the right to use any trademark, service
mark, tradename, or logo of the Copyright Holder.

This license includes the non-exclusive, worldwide, free-of-charge
patent license to make, have made, use, offer to sell, sell, import and
otherwise transfer the Package with respect to any patent claims
licensable by the Copyright Holder that are necessarily infringed by the
Package. If you institute patent litigation (including a cross-claim or
counterclaim) against any party alleging that the Package constitutes
direct or contributory patent infringement, then this Artistic License
to you shall terminate on the date that such litigation is filed.

Disclaimer of Warranty: THE PACKAGE IS PROVIDED BY THE COPYRIGHT HOLDER
AND CONTRIBUTORS "AS IS' AND WITHOUT ANY EXPRESS OR IMPLIED WARRANTIES.
THE IMPLIED WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
PURPOSE, OR NON-INFRINGEMENT ARE DISCLAIMED TO THE EXTENT PERMITTED BY
YOUR LOCAL LAW. UNLESS REQUIRED BY LAW, NO COPYRIGHT HOLDER OR
CONTRIBUTOR WILL BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, OR
CONSEQUENTIAL DAMAGES ARISING IN ANY WAY OUT OF THE USE OF THE PACKAGE,
EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

=cut

1; # End of DBIx::Crawl
