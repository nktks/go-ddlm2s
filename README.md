# go-ddlm2s
DDL converter from MySQL to Cloud Spanner

# how to run
go run cmd/ddlm2s/main.go -f db.sql

# convert details

ref:
https://cloud.google.com/solutions/migrating-mysql-to-spanner?hl=ja#supported_data_types

- convert all sql splitted by ;
- disable foreign key. alternatively, convert to interleave first foreign key.
  - change primary key `id` to singuler table_name id for interleave. (ie. users.id to users.user_id)
  - change id column and foreigin key of id key type to STRING(36) for uuid
- divide create index statement from create table statement.
- disable auto_increment, default value, engine, character set option because spanner does not support.
- convert column data type.
- ddlm2s donot support many other case...
  - ddlm2s only support create table list.
  - interleave needs parent primary key define. so ddlm2s needs all parent create table ddls for having foreign keys table ddl.

# example
```
CREATE TABLE `users` (
  `id`                   BIGINT (20) NOT NULL AUTO_INCREMENT,
  `name`                 VARCHAR (255) NOT NULL DEFAULT '',
  `uid`                  VARCHAR (255) NOT NULL,
  `created_at`           DATETIME NOT NULL,
  `updated_at`           DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE `idx_users_uid` (`uid`)
) ENGINE = InnoDB, DEFAULT CHARACTER SET = utf8mb4;

CREATE TABLE `friends` (
  `id`         BIGINT (20) NOT NULL AUTO_INCREMENT,
  `user_id`    BIGINT (20) NOT NULL,
  `to_id`  BIGINT (20) NOT NULL,
  `created_at` DATETIME NOT NULL,
  `updated_at` DATETIME NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE `idx_friends_user_id_to_id` (`user_id`, `to_id`),
  CONSTRAINT `fk_friends_users_1` FOREIGN KEY (`user_id`)   REFERENCES `users` (`id`),
  CONSTRAINT `fk_friends_users_2` FOREIGN KEY (`to_id`) REFERENCES `users` (`id`)
) ENGINE = InnoDB, DEFAULT CHARACTER SET = utf8mb4;

```
```
go run cmd/ddlm2s/main.go -f sample.sql
CREATE TABLE `users` (
	`user_id` STRING(36) NOT NULL,
	`name` STRING(255) NOT NULL,
	`uid` STRING(255) NOT NULL,
	`created_at` TIMESTAMP NOT NULL,
	`updated_at` TIMESTAMP NOT NULL
) PRIMARY KEY  (`user_id`);
CREATE UNIQUE INDEX idx_users_uid ON users (uid);
CREATE TABLE `friends` (
	`friend_id` STRING(36) NOT NULL,
	`user_id` STRING(36) NOT NULL,
	`to_id` STRING(36) NOT NULL,
	`created_at` TIMESTAMP NOT NULL,
	`updated_at` TIMESTAMP NOT NULL
) PRIMARY KEY  (`user_id`, `friend_id`),
INTERLEAVE IN PARENT `users` ;
CREATE UNIQUE INDEX idx_friends_user_id_to_id ON friends (user_id, to_id);```
```


