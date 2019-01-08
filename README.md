# NAME

DBIx::Crawl - fetch partial database content using predefined links.

# DESCRIPTION

The `DBIx::Crawl` module as well as its command-line interface `db-crawl.pl`
allows to define _links_ between database tables and fetch partial content
starting from certain rows.

These links may or may not correspond to foreign keys in the database.

# CONTENT OF THIS PACKAGE

* `bin/db-crawl.pl` - a program to fetch database content via a config file

* `example` - example configuration files & SQLite-based db schemas

* `lib` - modules

* `t` - tests

# INSTALLATION

To install this module, run the following commands:

	perl Makefile.PL
	make
	make test
	make install

To proceed without installation, just use `bin/db-crawl.pl`
and a configuration file.

# CONFIGURATION

See `perldoc bin/db-crawl.pl` for detailed configuration file format.

See [this example](example/artist.conf) for what it looks like.

See [examples readme](example/README.md) for how to use it.

# SUPPORT AND DOCUMENTATION

After installing, you can find documentation for this module with the
perldoc command.

    perldoc DBIx::Crawl

You can also look for information at:

*   [github](https://github.com/dallaylaen/dbix-crawl)

# LICENSE AND COPYRIGHT

Copyright (C) 2018-2019 Konstantin S. Uvarin

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

