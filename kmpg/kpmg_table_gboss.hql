CREATE EXTERNAL TABLE `db_billing.gboss_user_login_log`(
  `login_time` string,
  `product_id` string,
  `server_id` string,
  `user_id` string,
  `login_ip` string,
  `union_id` string,
  `ad_id` string,
  `platform` string,
  `game_code` string,
  `ldate` string
)
PARTITIONED BY (
  `region` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;

