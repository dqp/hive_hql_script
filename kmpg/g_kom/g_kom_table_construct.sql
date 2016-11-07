----------------------------------------------------------------
--未包含在init.sql的初始化语句。 e.g. 显示表头
----------------------------------------------------------------
set hive.cli.print.header=true;
----------------------------------------------------------------
--动态分区用初始化语句。
----------------------------------------------------------------
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions=200000;
set hive.exec.max.dynamic.partitions.pernode=200000;
----------------------------------------------------------------
--道具名称、描述对应表的建表语句；[公共表]
--注：分区 ds = '2016-08-01'
----------------------------------------------------------------
create table db_game_origin_mysql_backup.kabam_te_string
(
    category string,
    title string,
    `language` string,
    phrase string,
    lastupdated string
)
partitioned by
(
    ds string
)
ROW FORMAT DELIMITED 
  FIELDS TERMINATED BY '`' ;

