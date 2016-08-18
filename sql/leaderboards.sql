CREATE TABLE `leaderboards` (
  `leaderboard_id` int(11) NOT NULL AUTO_INCREMENT,
  `leaderboard_name` varchar(45) NOT NULL,
  `gamespace_id` int(11) NOT NULL,
  `leaderboard_sort_order` enum('asc','desc') NOT NULL DEFAULT 'asc',
  PRIMARY KEY (`leaderboard_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8;