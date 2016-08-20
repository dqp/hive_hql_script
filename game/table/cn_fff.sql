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



