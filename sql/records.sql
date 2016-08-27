CREATE TABLE `records` (
  `record_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `account_id` int(11) unsigned NOT NULL,
  `gamespace_id` int(11) unsigned NOT NULL,
  `leaderboard_id` int(11) unsigned NOT NULL,
  `expire_at` timestamp NOT NULL,
  `score` float DEFAULT NULL,
  `display_name` varchar(45) NOT NULL,
  `profile` json NOT NULL,
  PRIMARY KEY (`record_id`),
  KEY `leaderboard_id_idx` (`leaderboard_id`),
  CONSTRAINT `leaderboard_id` FOREIGN KEY (`leaderboard_id`) REFERENCES `leaderboards` (`leaderboard_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;