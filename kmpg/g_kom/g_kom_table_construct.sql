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



----------------------------------------------------------------
--Kabam 霍比特人 KOM userid对应付费金额(USD)和充值获得钻石数
----------------------------------------------------------------
--账号付费、获得虚拟币数量>建表，用于取重要玩家
create table kp_gaea_audit.kabam_kom_key_user_gem
(
    userid string,
    payamount string,
    gem string
);
--账号付费、获得虚拟币数量>建表(数据)，用于取重要玩家
insert into kp_gaea_audit.kabam_kom_key_user_gem
select userid, sum(cents)/100.0 as payamount, sum(gems) as gem
from db_game_origin_mysql_backup.gaea_g_kom_dollar_transactions
where ds = '2016-10-11'
    and substring(createddate,1,10) between '2016-01-01' and '2016-09-30'
group by userid;

