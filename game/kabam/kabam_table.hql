CREATE EXTERNAL TABLE `gaea_g_kom_dollar_transactions`(
  `dollartransactionid` string,
  `type` string,
  `userid` string,
  `serverid` string,
  `productid` string,
  `ordernumber` string,
  `offercategory` string,
  `flow` string,
  `paymentservice` string,
  `cents` string,
  `localprice` string,
  `gems` string,
  `superrewardtotal` string,
  `createddate` string,
  `editeddate` string,
  `mobileid` string,
  `updated` string)
PARTITIONED BY (
  `ds` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;




CREATE EXTERNAL TABLE `gaea_g_kom_gem_account`(
  `userid` string,
  `availablegems` string,
  `purchasedgems` string,
  `spentgems` string,
  `freegems` string,
  `freegembalance` string,
  `wongems` string,
  `centamount` string,
  `newcentamount` string,
  `superrewardtotal` string,
  `firstdate` string,
  `lastdate` string,
  `numpurchases` string,
  `editeddate` string)
PARTITIONED BY (
  `ds` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `gaea_g_kom_gem_transactions`(
  `gemtransactionid` string,
  `userid` string,
  `sourceid` string,
  `drainid` string,
  `sourcetype` string,
  `draintype` string,
  `serverid` string,
  `transactiontype` string,
  `proposedamount` string,
  `acceptedamount` string,
  `acceptedrealamount` string,
  `acceptedworldamount` string,
  `centamount` string,
  `newcentamount` string,
  `localprice` string,
  `createddate` string,
  `editeddate` string,
  `fullfilled` string,
  `seen` string,
  `transactionbonusid` string,
  `might` string,
  `title` string,
  `closinggembalance` string,
  `cnt_logins` string,
  `citycount` string,
  `itemcount` string,
  `updated` string)
PARTITIONED BY (
  `ds` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `gaea_g_kom_user`(
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


CREATE EXTERNAL TABLE `gaea_g_kom_daily_log_data`(
  `date` string,
  `userid` string,
  `logincount` string,
  `entrychannel` string,
  `successlogincount` string,
  `hotlogincount` string,
  `warmlogincount` string)
PARTITIONED BY (
  `ds` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `gaea_g_kbn_daily_log_data`(
  `date` string,
  `userid` string,
  `logincount` string,
  `entrychannel` string,
  `successlogincount` string,
  `hotlogincount` string,
  `warmlogincount` string)
PARTITIONED BY (
  `ds` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `gaea_g_kbn_dollar_transactions`(
  `dollartransactionid` string,
  `type` string,
  `userid` string,
  `serverid` string,
  `productid` string,
  `ordernumber` string,
  `offercategory` string,
  `flow` string,
  `paymentservice` string,
  `cents` string,
  `localprice` string,
  `gems` string,
  `superrewardtotal` string,
  `createddate` string,
  `editeddate` string,
  `mobileid` string,
  `reducegems` string)
PARTITIONED BY (
  `ds` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `gaea_g_kbn_gem_account`(
  `userid` string,
  `availablegems` string,
  `purchasedgems` string,
  `spentgems` string,
  `freegems` string,
  `freegembalance` string,
  `wongems` string,
  `centamount` string,
  `newcentamount` string,
  `superrewardtotal` string,
  `firstdate` string,
  `lastdate` string,
  `numpurchases` string,
  `editeddate` string,
  `reducegems` string)
PARTITIONED BY (
  `ds` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `gaea_g_kbn_gem_transactions`(
  `gemtransactionid` string,
  `userid` string,
  `sourceid` string,
  `drainid` string,
  `sourcetype` string,
  `draintype` string,
  `serverid` string,
  `transactiontype` string,
  `proposedamount` string,
  `acceptedamount` string,
  `acceptedrealamount` string,
  `centamount` string,
  `newcentamount` string,
  `localprice` string,
  `createddate` string,
  `editeddate` string,
  `fullfilled` string,
  `seen` string,
  `transactionbonusid` string,
  `might` string,
  `title` string,
  `closinggembalance` string,
  `cnt_logins` string,
  `citycount` string,
  `acceptedshadowamount` string,
  `itemcount` string)
PARTITIONED BY (
  `ds` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `gaea_g_kbn_user`(
  `userid` string,
  `email` string,
  `username` string,
  `passwd` string,
  `mobileid` string,
  `version` string,
  `naid` string,
  `access_token` string,
  `code` string,
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
  `country_code` string,
  `cents` string,
  `centsupdatetime` string,
  `last_login_offer` string,
  `isadmin` string)
PARTITIONED BY (
  `ds` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;



CREATE EXTERNAL TABLE `gaea_g_kbn_world_gem_account` (
  `userId` string,
  `availableGems` string,
  `totalGems` string,
  `spentGems` string
)
PARTITIONED BY (
  `ds` string,
  `serverid` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `gaea_g_kom_world_gem_account` (
  `userId` string,
  `availableGems` string,
  `spentGems` string,
  `freeGems` string,
  `freeGemBalance` string,
  `wonGems` string,
  `superRewardTotal` string,
  `firstDate` string,
  `lastDate` string,
  `numPurchases` string,
  `editedDate` string
)
PARTITIONED BY (
  `ds` string,
  `serverid` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;



CREATE TABLE `gaea_g_kom_world_gem_transactions` (
  `gemTransactionId` string,
  `userId` string,
  `sourceId` string,
  `drainId` string,
  `sourceType` string,
  `drainType` string,
  `transactionType` string,
  `proposedAmount` string,
  `acceptedAmount` string,
  `createdDate` string,
  `editedDate` string,
  `fullfilled` string,
  `transactionBonusId` string,
  `itemAmount` string
)
PARTITIONED BY (
  `ds` string,
  `serverid` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;



CREATE EXTERNAL TABLE `gaea_g_kbn_s_world_gem_account` (
  `userId` string,
  `availableGems` string,
  `totalGems` string,
  `spentGems` string
)
PARTITIONED BY (
  `ds` string,
  `serverid` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;


CREATE EXTERNAL TABLE `gaea_g_kom_s_world_gem_account` (
  `userId` string,
  `availableGems` string,
  `spentGems` string,
  `freeGems` string,
  `freeGemBalance` string,
  `wonGems` string,
  `superRewardTotal` string,
  `firstDate` string,
  `lastDate` string,
  `numPurchases` string,
  `editedDate` string
)
PARTITIONED BY (
  `ds` string,
  `serverid` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;



CREATE TABLE `gaea_g_kom_s_world_gem_transactions` (
  `gemTransactionId` string,
  `userId` string,
  `sourceId` string,
  `drainId` string,
  `sourceType` string,
  `drainType` string,
  `transactionType` string,
  `proposedAmount` string,
  `acceptedAmount` string,
  `createdDate` string,
  `editedDate` string,
  `fullfilled` string,
  `transactionBonusId` string,
  `itemAmount` string
)
PARTITIONED BY (
  `ds` string,
  `serverid` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;

