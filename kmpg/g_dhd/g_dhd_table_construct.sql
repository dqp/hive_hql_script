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
--Kabam 龙族崛起 DHD 用户付费表，加虚拟币获得数>用于取重要玩家
----------------------------------------------------------------
--Kabam 龙族崛起 DHD 用户付费表，加虚拟币获得数，建表(结构) 新
create table kp_gaea_audit.kabam_dhd_key_user 
(
    userid string,
    payamount string,
    vcurrency string    
)
stored as orc;

--Kabam 龙族崛起 DHD 用户付费表，加虚拟币获得数，建表(数据) 新 #33931
insert into table kp_gaea_audit.kabam_dhd_key_user
select user_id as userid, sum(cast(amount_cents as bigint))/100.0 as payamount, sum(cast(`value` as bigint)) as vcurrency
from db_game_g_dhd.gaea_g_dhd_payments
where ds between '2016-01-01' and '2016-09-30'
group by user_id
order by payamount desc;


---- 20160101~20160930 玩家付费表
create external table kp_gaea_audit.g_dhd_payers_201609
(
    userid string,
    payamount string,
    vcurrency string    
)
stored as parquet;

insert into table kp_gaea_audit.g_dhd_payers_201609
select user_id as userid, sum(cast(amount_cents as bigint))/100.0 as payamount, sum(cast(`value` as bigint)) as vcurrency
from db_game_g_dhd.gaea_g_dhd_payments
where ds between '2016-01-01' and '2016-09-30'
group by user_id
order by payamount desc;





