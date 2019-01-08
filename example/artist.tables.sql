-- tested with SQLite only

CREATE TABLE artist (
    id              INTEGER PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    bio             TEXT
);

CREATE TABLE album (
    id              INTEGER PRIMARY KEY,
    artist_id       INTEGER NOT NULL REFERENCES artist,
    title           VARCHAR(255) NOT NULL,
    released        DATE
);

-- composite primary key here, which is bad
CREATE TABLE song (
    album_id        INTEGER NOT NULL REFERENCES album,
    track_number    SMALLINT NOT NULL,
    title           VARCHAR(255) NOT NULL,
    written_by      INTEGER -- artist_id
);

CREATE UNIQUE INDEX idx_song ON song(album_id,track_number);
