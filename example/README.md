# ARTIST-ALBUM-SONG example

## Create database

    sqlite3 mydb.sqlite <example/artist.tables.sql
    sqlite3 mydb.sqlite <example/artist.data.sql

## Select all artist, no albums

    bin/db-crawl.pl --config example/artist.conf --db mydb.sqlite artist:all

## Animals only

    bin/db-crawl.pl --config example/artist.conf --db mydb.sqlite album:id=1

## Animals, again, because song and album are linked in both directions

    bin/db-crawl.pl --config example/artist.conf --db mydb.sqlite song:title=Pigs

