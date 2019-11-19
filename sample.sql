CREATE TABLE `users` (
  `id`                   VARCHAR (36) NOT NULL DEFAULT '',
  `name`                 VARCHAR (255) NOT NULL DEFAULT '',
  `uid`                  VARCHAR (255) NOT NULL,
  `created_at`           DATETIME NOT NULL,
  `updated_at`           DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE `idx_users_uid` (`uid`)
) ENGINE = InnoDB, DEFAULT CHARACTER SET = utf8mb4;

CREATE TABLE `friends` (
  `id`         BIGINT (20) NOT NULL AUTO_INCREMENT,
  `user_id`    VARCHAR (36) NOT NULL,
  `to_id`      VARCHAR (36) NOT NULL,
  `created_at` DATETIME NOT NULL,
  `updated_at` DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE `idx_friends_user_id_to_id` (`user_id`, `to_id`),
  CONSTRAINT `fk_friends_users_1` FOREIGN KEY (`user_id`)   REFERENCES `users` (`id`),
  CONSTRAINT `fk_friends_users_2` FOREIGN KEY (`to_id`) REFERENCES `users` (`id`)
) ENGINE = InnoDB, DEFAULT CHARACTER SET = utf8mb4;
