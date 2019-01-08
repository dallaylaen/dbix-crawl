BEGIN;
        --;-- insert SQL here

INSERT INTO `album`(`artist_id`, `id`, `released`, `title`) VALUES ('2', '2', NULL, 'Ommadawn');
INSERT INTO `album`(`artist_id`, `id`, `released`, `title`) VALUES ('1', '1', NULL, 'Animals');
INSERT INTO `artist`(`bio`, `id`, `name`) VALUES (NULL, '2', 'Mike Oldfield');
INSERT INTO `artist`(`bio`, `id`, `name`) VALUES (NULL, '1', 'Pink Floyd');
INSERT INTO `song`(`album_id`, `title`, `track_number`, `written_by`) VALUES ('2', 'Part I', '1', NULL);
INSERT INTO `song`(`album_id`, `title`, `track_number`, `written_by`) VALUES ('2', 'On Horseback', '3', NULL);
INSERT INTO `song`(`album_id`, `title`, `track_number`, `written_by`) VALUES ('1', 'Pigs on the wing II', '5', NULL);
INSERT INTO `song`(`album_id`, `title`, `track_number`, `written_by`) VALUES ('2', 'Part II', '2', NULL);
INSERT INTO `song`(`album_id`, `title`, `track_number`, `written_by`) VALUES ('1', 'Dogs', '2', NULL);
INSERT INTO `song`(`album_id`, `title`, `track_number`, `written_by`) VALUES ('1', 'Pigs on the wing', '1', NULL);
INSERT INTO `song`(`album_id`, `title`, `track_number`, `written_by`) VALUES ('1', 'Pigs', '3', NULL);
INSERT INTO `song`(`album_id`, `title`, `track_number`, `written_by`) VALUES ('1', 'Sheep', '4', NULL);
COMMIT;
