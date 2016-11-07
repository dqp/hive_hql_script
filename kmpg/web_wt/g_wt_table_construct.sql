CREATE EXTERNAL TABLE `db_game_web_wt.gaea_web_wt_charge`
(
    `orderid` string,
    `roleid` string,
    `udid` string,
    `rolename` string,
    `paychannel` string,
    `currency` string,
    `diamonds` string,
    `currencytype` string,
    `time` string,
    `serverid` string
)PARTITIONED BY
(
    ds string
);

ds=from_unixtime(unix_timestamp(`time`,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')


----------------------------------------------------------------
--使用CSV SerDe - 源数据 最新在分区 ds = '2016-11-07'
----------------------------------------------------------------
CREATE EXTERNAL TABLE `db_game_origin_mysql_backup.gaea_web_wt_charge`
(
    `orderid` string,
    `roleid` string,
    `udid` string,
    `rolename` string,
    `paychannel` string,
    `currency` string,
    `diamonds` string,
    `currencytype` string,
    `time` string,
    `serverid` string
)
PARTITIONED BY
(
    ds string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
    "separatorChar" = ",",
    "quoteChar"     = "\"",
    "escapeChar"    = "\\"
)  
STORED AS TEXTFILE;
----------------------------------------------------------------
--使用CSV SerDe - 分区 - 建表
----------------------------------------------------------------
CREATE EXTERNAL TABLE `db_game_web_wt.gaea_web_wt_charge`
(
    `orderid` string,
    `roleid` string,
    `udid` string,
    `rolename` string,
    `paychannel` string,
    `currency` string,
    `diamonds` string,
    `currencytype` string,
    `time` string,
    `serverid` string
)
PARTITIONED BY
(
    ds string
);
----------------------------------------------------------------
--使用CSV SerDe - 分区 - 数据
----------------------------------------------------------------
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=200000;

insert into table db_game_web_wt.gaea_web_wt_charge partition (ds)
select orderid, roleid, udid, rolename, paychannel, currency, diamonds,
    currencytype, `time`, serverid,
    from_unixtime(unix_timestamp(`time`,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd') as ds
from db_game_origin_mysql_backup.gaea_web_wt_charge
where ds = '2016-11-07';

----------------------------------------------------------------
--去掉脏数据!!
----------------------------------------------------------------
alter table db_game_web_wt.gaea_web_wt_charge
drop partition (ds = '__HIVE_DEFAULT_PARTITION__');
hdfs dfs -rm -r /user/hive/warehouse/db_game_web_wt.db/gaea_web_wt_charge/ds=__HIVE_DEFAULT_PARTITION__
----------------------------------------------------------------
--分区验证!!
----------------------------------------------------------------
select ds, count(orderid), count(distinct orderid)
from db_game_web_wt.gaea_web_wt_charge
group by ds
having count(orderid) != count(distinct orderid)
order by ds;
=nothing
