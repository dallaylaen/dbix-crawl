package DBIx::Slice;

use 5.006;
use strict;
use warnings;

=head1 NAME

DBIx::Slice - The great new DBIx::Slice!

=head1 VERSION

Version 0.01

=cut

our $VERSION = '0.01';


=head1 SYNOPSIS

Quick summary of what the module does.

Perhaps a little code snippet.

    use DBIx::Slice;

    my $foo = DBIx::Slice->new();
    ...

=head1 EXPORT

A list of functions that can be exported.  You can delete this section
if you don't export anything, such as for a purely object-oriented module.

=head1 SUBROUTINES/METHODS

=cut

use Moo;

has keys   => is => "rw", default => sub { {} };

# table => field => [ table : field, ... ]
has links  => is => "rw", default => sub { {} };

# table => [ {record}, ... ]
has data   => is => "rw", default => sub { {} };

# table => 'keypair' => 'valuepair'
has seen => is => "rw", default => sub { {} };


has override => is => "rw", default => sub { {} };

# DDL

sub add_link {
    my ($self, $table, $fk, $ref, $pk) = @_;

    $pk ||= $self->keys->{$ref}[0];

    $self->links->{$table}{$fk}{$ref.'.'.$pk}++;
    return $self;
};

sub add_table {
    my ($self, $table, $key) = @_;

    $self->keys->{$table} = ref $key eq 'ARRAY' ? $key : [$key];
};

sub add_override {
    my ($self, $table, $code) = @_;

    $self->override->{$table} = $code;
};

# Pure

sub make_key {
    my ($self, $data) = @_;

    my @keys = sort keys %$data;
    return ( $self->joinf( @keys ), $self->joinf( map { $data->{$_} } @keys ) );
};

sub make_primary_key {
    my ($self, $table, $data) = @_;

    my $keys = $self->keys->{$table};

    return ( $self->joinf( @$keys ), $self->joinf( map { $data->{$_} } @$keys ) );
};

# TODO configurable delimiter
sub joinf {
    my $self = shift;

    return join "\t", @_;
};

sub splitf {
    my ($self, $row) = @_;

    return split /\t/, $row;
};

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

# Data

sub clear {
    my $self = shift;
    $self->data({});
    $self->seen({});
    return $self;
};

sub is_seen {
    my ($self, $table, $data) = @_;

    my ($key, $value) = $self->make_key( $data );

    return $self->seen->{$table}{$key}{$value};
};

sub mark_seen {
    my ($self, $table, $data) = @_;

    my ($key, $value) = $self->make_key( $data );

    return $self->seen->{$table}{$key}{$value}++;
};

sub add_data {
    my ($self, $table, $data) = @_;

    my (undef, $key) = $self->make_primary_key( $table, $data );

    $self->data->{$table}{$key} = $data;
    return $self;
};

# DBI

sub fetch_one {
    my ($self, $dbh, $table, $key) = @_;

    my @args;
    my $sql = "SELECT * FROM `$table` WHERE 1";
    foreach (keys %$key) {
        # TODO if defined
        $sql .= " AND `$_` = ?";
        push @args, $key->{$_};
    };

    warn "\tfetch: $sql [@args]";

    my $sth = $dbh->prepare( $sql );
    $sth->execute(@args);

    my @ret;
    while (my $row = $sth->fetchrow_hashref) {
        push @ret, $row;
    };
    return @ret;
};

sub fetch {
    my ($self, $dbh, @todo) = @_;

    while (@todo) {
        my $req = shift @todo;
        my ($table, $key) = @$req;

        my $seen = $self->mark_seen( $table, $key );
        my @req = $self->make_key( $key );

        warn "\tfetching from $table where ($req[0]) = ($req[1]), seen=$seen";
        next if $seen;

        my @data = $self->fetch_one( $dbh, $table, $key );

        warn "\tGot items: ", scalar @data;

        foreach (@data) {
            $self->add_data($table => $_);
            push @todo, $self->get_linked( $table => $_ );
        };
    };
};

sub insert_one {
    my ($self, $dbh, $table, $data) = @_;

    if (my $code = $self->override->{$table} ) {
        $data = { %$data }; # shallow copy
        $code->( $data, $table );
    };

    my @fields = sort grep { defined $data->{$_} } keys %$data;
    my $fields = join ",", map { "`$_`" } @fields;
    my $quest  = join ",", map { "?" } @fields;
    my @args   = map { $data->{$_} } @fields;

    my $sql = "INSERT INTO `$table`($fields) VALUES( $quest )";

    warn "\t$sql [@args]";

    my $sth = $dbh->prepare( $sql );
    $sth->execute( @args );
};

sub insert {
    my ($self, $dbh) = @_;

    my $data = $self->data;

    my @todo;
    foreach my $table( keys %$data ) {
        foreach my $item ( values %{$data->{$table}} ) {
            push @todo, [ $table, $item ];
        };
    };

    $dbh->begin_work;
    foreach (@todo) {
        $self->insert_one( $dbh, @$_ );
    };
    $dbh->commit;

};

=head1 AUTHOR

Konstantin S. Uvarin, C<< <khedin at gmail.com> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-dbix-slice at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=DBIx-Slice>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.


=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc DBIx::Slice


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker (report bugs here)

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=DBIx-Slice>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/DBIx-Slice>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/DBIx-Slice>

=item * Search CPAN

L<http://search.cpan.org/dist/DBIx-Slice/>

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

1; # End of DBIx::Slice
