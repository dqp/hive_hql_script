---- put file to HDFS
#$ hadoop fs -copyFromLocal db_game_cn_fff.gaea_cn_fff_data_charge_log/* /user/hive/warehouse/db_game_origin_mysql_backup.db/gaea_cn_fff_data_charge_log/
#$ hadoop fs -copyFromLocal db_game_cn_fff.gaea_cn_fff_data_summoner/* /user/hive/warehouse/db_game_origin_mysql_backup.db/gaea_cn_fff_data_summoner/

msck repair talbe db_game_origin_mysql_backup.gaea_cn_fff_data_charge_log;
msck repair table db_game_origin_mysql_backup.gaea_cn_fff_data_summoner;

---- construct table charge_log
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
INSERT OVERWRITE table `db_game_cn_fff.gaea_cn_fff_data_charge_log` PARTITION (ds, serverid)
select order_channel, orderid_channel, channelaccount, date_time, productid, last_login_time, result, pay_money_fen, normal_addcredit, add_credit, keep_time_sec, sumid, substring(date_time, 1, 10) as ds, serverid
from `db_game_origin_mysql_backup.gaea_cn_fff_data_charge_log`
where ds = '2016-11-04'
    and substring(date_time, 1, 10) between '2016-07-01' and '2016-10-30';


---- construct table data_summoner

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
INSERT OVERWRITE table `db_game_cn_fff.gaea_cn_fff_data_summoner` PARTITION (serverid)
select id, accountid, serverid
from `db_game_origin_mysql_backup.gaea_cn_fff_data_summoner`
where ds = '2016-11-04';



---
CREATE EXTERNAL TABLE `db_game_cn_fff.gaea_cn_fff_data_summoner`(
  `id` string,
  `accountid` string)
PARTITIONED BY (
  `serverid` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS PARQUET;