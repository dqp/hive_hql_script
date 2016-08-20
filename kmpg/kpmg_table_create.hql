CREATE EXTERNAL TABLE `cn_sbkcq_key_users`(
  `third_id` string,
  `third_string` string,
  `rmb` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `kp_gaea_audit.cn_sbkcq_key_users_type`(
  `third_id` string,
  `third_type` string,
  `reg_time` string,
  `rmb` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE TABLE `kp_gaea_audit.cn_fff_users_payment_1`(
  `channel` string,
  `user_id` string,
  `pay_currency` string,
  `pay_amount` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `gaea_kr_dota_diamond_flow_backup`(
  `ieventid` string,
  `dteventtime` string,
  `iuserid` string,
  `vusername` string,
  `ilevel` string,
  `icharge` string,
  `iaction` string,
  `iflow` string,
  `idiamondbefore` string,
  `idiamondafter` string)
PARTITIONED BY (
  `ds` string,
  `serverid` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `gaea_kr_dota_login_backup`(
  `ieventid` string,
  `dteventtime` string,
  `iuserid` string,
  `vusername` string,
  `vloginip` string,
  `ilevel` string,
  `icharge` string,
  `vfrom` string,
  `vuin` string)
PARTITIONED BY (
  `ds` string,
  `serverid` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `gaea_kr_dota_user_create_backup`(
  `ieventid` string,
  `dteventtime` string,
  `iuserid` string,
  `vusername` string,
  `vloginip` string,
  `vuin` string,
  `vdevid` string,
  `iisnewuin` string)
PARTITIONED BY (
  `ds` string,
  `serverid` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;