CREATE TABLE `records` (
  `record_id` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `account_id` int(11) unsigned NOT NULL,
  `cluster_id` int(11) unsigned DEFAULT NULL,
  `gamespace_id` int(11) unsigned NOT NULL,
  `leaderboard_id` int(11) unsigned NOT NULL,
  `expire_at` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00' ON UPDATE CURRENT_TIMESTAMP,
  `score` float DEFAULT NULL,
  `display_name` varchar(45) NOT NULL,
  `profile` json NOT NULL,
  PRIMARY KEY (`record_id`),
  KEY `leaderboard_id_idx` (`leaderboard_id`),
  KEY `score` (`score`),
  KEY `cluster_id` (`cluster_id`),
  KEY `leaderboard_id` (`leaderboard_id`),
  CONSTRAINT `leaderboard_id` FOREIGN KEY (`leaderboard_id`) REFERENCES `leaderboards` (`leaderboard_id`) ON DELETE NO ACTION ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;