CREATE EXTERNAL TABLE `db_game_kr_dota.gaea_kr_dota_login`(
  `ieventid` string,
  `dteventtime` string,
  `iuserid` string,
  `vusername` string,
  `vloginip` string,
  `ilevel` string,
  `icharge` string,
  `vfrom` string,
  `vuin` string
)
PARTITIONED BY (
  `ds` string,
  `serverid` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `db_game_kr_dota.gaea_kr_dota_diamond_flow`(
  `ieventid` string,
  `dteventtime` string,
  `iuserid` string,
  `vusername` string,
  `ilevel` string,
  `icharge` string,
  `iaction` string,
  `iflow` string,
  `idiamondbefore` string,
  `idiamondafter` string
  )
PARTITIONED BY (
  `ds` string,
  `serverid` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;




CREATE EXTERNAL TABLE `db_game_kr_dota.gaea_kr_dota_user_create`(
  `ieventid` string,
  `dteventtime` string,
  `iuserid` string,
  `vusername` string,
  `vloginip` string,
  `vuin` string,
  `vdevid` string,
  `iisnewuin` string
  )
PARTITIONED BY (
  `ds` string,
  `serverid` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;





CREATE EXTERNAL TABLE `db_game_jp_dota.gaea_jp_dota_login`(
  `ieventid` string,
  `dteventtime` string,
  `iuserid` string,
  `vusername` string,
  `vloginip` string,
  `ilevel` string,
  `icharge` string,
  `vfrom` string,
  `vuin` string
)
PARTITIONED BY (
  `ds` string,
  `serverid` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;



CREATE EXTERNAL TABLE `db_game_jp_dota.gaea_jp_dota_diamond_flow`(
  `ieventid` string,
  `dteventtime` string,
  `iuserid` string,
  `vusername` string,
  `ilevel` string,
  `icharge` string,
  `iaction` string,
  `iflow` string,
  `idiamondbefore` string,
  `idiamondafter` string
  )
PARTITIONED BY (
  `ds` string,
  `serverid` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `db_game_jp_dota.gaea_jp_dota_user_create`(
  `ieventid` string,
  `dteventtime` string,
  `iuserid` string,
  `vusername` string,
  `vloginip` string,
  `vuin` string,
  `vdevid` string,
  `iisnewuin` string
  )
PARTITIONED BY (
  `ds` string,
  `serverid` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;




CREATE TABLE `db_game_cn_fff.gaea_cn_fff_data_charge_log` (
  `order_channel` string,
  `orderid_channel` string,
  `channelaccount` string,
  `date_time` string,
  `productid` string,
  `last_login_time` string,
  `result` string,
  `pay_money_fen` string,
  `normal_addcredit` string,
  `add_credit` string,
  `keep_time_sec` string,
  `sumid` string
  )
PARTITIONED BY (
  `ds` string,
  `serverid` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;



CREATE EXTERNAL TABLE `db_game_cn_fff.gaea_cn_fff_data_summoner`(
  `id` string,
  `accountid` string)
PARTITIONED BY (
  `ds` string,
  `serverid` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;



CREATE EXTERNAL TABLE `db_game_web_wt.gaea_web_wt_charge`(
  `orderid` string,
  `roleid` string,
  `udid` string,
  `rolename` string,
  `paychannel` string,
  `currency` string,
  `diamonds` string,
  `currencytype` string,
  `time` string,
  `serverid` string,
  `idate` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS textfile;



CREATE EXTERNAL TABLE `gaea_jp_dota_diamond_flow`(
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

CREATE EXTERNAL TABLE `gaea_jp_dota_login`(
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



CREATE EXTERNAL TABLE `gaea_kr_dota_user_create`(
  `ieventid` string,
  `dteventtime` string,
  `iuserid` string,
  `vusername` string,
  `vloginip` string,
  `vuin` string,
  `vdevid` string,
  `iisnewuin` string
  )
PARTITIONED BY (
  `ds` string,
  `serverid` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;



CREATE EXTERNAL TABLE `gaea_jp_dota_user_create`(
  `ieventid` string,
  `dteventtime` string,
  `iuserid` string,
  `vusername` string,
  `vloginip` string,
  `vuin` string,
  `vdevid` string,
  `iisnewuin` string
  )
PARTITIONED BY (
  `ds` string,
  `serverid` string
  )
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;



CREATE TABLE `gaea_cn_fff_data_charge_log`(
  `order_channel` string,
  `orderid_channel` string,
  `channelaccount` string,
  `date_time` string,
  `productid` string,
  `last_login_time` string,
  `result` string,
  `pay_money_fen` string,
  `normal_addcredit` string,
  `add_credit` string,
  `keep_time_sec` string,
  `sumid` string)
PARTITIONED BY (
  `ds` string,
  `serverid` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `gaea_cn_fff_data_summoner`(
  `id` string,
  `accountid` string)
PARTITIONED BY (
  `ds` string,
  `serverid` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;



CREATE EXTERNAL TABLE `kom_user`(
  `userid` string,
  `email` string,
  `username` string,
  `passwd` string,
  `mobileid` string,
  `version` string,
  `fbuid` string,
  `kabam_id` string,
  `gcuid` string,
  `gcunick` string,
  `from_kabam_com` string,
  `firstname` string,
  `lastname` string,
  `gender` string,
  `usertype` string,
  `status` string,
  `highesttitle` string,
  `cnt_newmsg` string,
  `cnt_friendreq` string,
  `cnt_logins` string,
  `cnt_fblogins` string,
  `date_joined` string,
  `last_login` string,
  `last_success_login` string,
  `ip_address` string,
  `recentserverid` string,
  `entrytag` string,
  `entrychannel` string,
  `bdayyear` string,
  `bdaymonth` string,
  `bdayday` string,
  `locale` string,
  `uselang` string,
  `degree` string,
  `inviterid` string,
  `genesisuserid` string,
  `progenycount` string,
  `naid` string,
  `access_token` string,
  `code` string,
  `country_code` string,
  `cents` string,
  `centsupdatetime` string,
  `updated` string)
PARTITIONED BY (
  `ds` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;




create EXTERNAL TABLE gaea_g_kom_user(
  userid string,
  email string,
  mobileid string,
  fbuid string,
  kabam_id string,
  from_kabam_com string,
  usertype string,
  status string,
  highesttitle string,
  cnt_newmsg string,
  cnt_friendreq string,
  cnt_logins string,
  cnt_fblogins string,
  date_joined string,
  last_login string,
  last_success_login string,
  ip_address string,
  recentserverid string,
  entrytag string,
  entrychannel string,
  bdayyear string,
  bdaymonth string,
  bdayday string,
  locale string,
  uselang string,
  degree string,
  inviterid string,
  genesisuserid string,
  progenycount string,
  naid string,
  access_token string,
  code string,
  country_code string,
  cents string,
  centsupdatetime string,
  updated string
)PARTITIONED BY (
  `ds` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


create EXTERNAL TABLE gaea_g_kbn_user(
  userid string,
  email string,
  mobileid string,
  fbuid string,
  kabam_id string,
  from_kabam_com string,
  usertype string,
  status string,
  highesttitle string,
  cnt_newmsg string,
  cnt_friendreq string,
  cnt_logins string,
  cnt_fblogins string,
  date_joined string,
  last_login string,
  last_success_login string,
  ip_address string,
  recentserverid string,
  entrytag string,
  entrychannel string,
  bdayyear string,
  bdaymonth string,
  bdayday string,
  locale string,
  uselang string,
  degree string,
  inviterid string,
  genesisuserid string,
  progenycount string,
  naid string,
  access_token string,
  code string,
  country_code string,
  cents string,
  centsupdatetime string
)PARTITIONED BY (
  `ds` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;

create external table gaea_g_kom_server_merge_plan(
  `planId` string,
  `sourceServerIds` string,
  `targetServerId` string,
  `noticeBeginTime` string,
  `maintenanceBeginTime` string,
  `setMaintainance` string,
  `note` string,
  `createdTime` string,
  `updatedTime` string
)PARTITIONED BY (
  `ds` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


create external table gaea_g_kom_server_merge_player_choice(
  `recordId` string,
  `playerId` string,
  `sourceServerId` string,
  `targetServerId` string,
  `planId` string,
  `fromServerIdList` string,
  `createdTime` string,
  `updatedTime` string,
  `recordStatus` string,
  `popedWelcome` string
)PARTITIONED BY (
  `ds` string,
  `batch` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;