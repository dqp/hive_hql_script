---- put file to HDFS
#$ hadoop fs -copyFromLocal db_game_cn_fff.gaea_cn_fff_data_charge_log/* /user/hive/warehouse/db_game_origin_mysql_backup.db/gaea_cn_fff_data_charge_log/
#$ hadoop fs -copyFromLocal db_game_cn_fff.gaea_cn_fff_data_summoner/* /user/hive/warehouse/db_game_origin_mysql_backup.db/gaea_cn_fff_data_summoner/

msck repair talbe db_game_origin_mysql_backup.gaea_kr_dota_diamond_flow;
msck repair table db_game_origin_mysql_backup.gaea_kr_dota_login;
msck repair table db_game_origin_mysql_backup.gaea_kr_dota_user_create;


CREATE external TABLE `db_game_kr_dota.gaea_kr_dota_user`(
    iUserId string,
    vDeviceId string,
    iLevel string,
    iCharge string,
    llGold string,
    iDiamond string,
    iServerId string,
    dtLastLoginTime string,
    iIsMigrate string
)
PARTITIONED BY (
  `ds` string,
  `serverid` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS textfile;



CREATE external TABLE `kp_gaea_audit.kr_dota_payers_201609`(
  `user_id` string,
  `pay_cny` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS PARQUET;


insert overwrite table kp_gaea_audit.kr_dota_payers_201609
select gboss.user_id, sum(gboss.pay_amount * x.rate) from
(
    select pay_currency, pay_amount, user_id
    from db_billing.gboss_pay_orders
    where region = 'kr' 
        and pay_state = '2'
        and from_unixtime(cast(pay_time as bigint), 'yyyyMM') <= '201609'
        and product_id in ('1020006','1010006','1010009','1020009')
) gboss
join
(
    select * 
    from kp_gaea_audit.currency_exchange_rate
) x
on (gboss.pay_currency = x.from_currency)
group by gboss.user_id;



select count(distinct user_id), sum(cast(pay_cny as double))
from kp_gaea_audit.kr_dota_payers_201609
where cast(pay_cny as double) > 5000; 


---- 重要玩家表创建
CREATE TABLE `kr_dota_key_users`(
    `user_id` string,
    `pay_cny` string
);


－－－ kr_dota done
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions = 100000;

---- from db_game_origin_mysql_backup
insert overwrite table db_game_kr_dota.gaea_kr_dota_diamond_flow partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,ilevel,icharge,iaction,iflow,idiamondbefore,idiamondafter,substring(dteventtime, 1, 10),serverid
from db_game_origin_mysql_backup.gaea_kr_dota_diamond_flow
where ds = '2016-07-18' and substring(dteventtime, 1, 10) between '2016-02-01' and '2016-02-28';


insert overwrite table db_game_kr_dota.gaea_kr_dota_login partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,vloginip,ilevel,icharge,vfrom,vuin,substring(dteventtime,1 ,10),serverid
from db_game_origin_mysql_backup.gaea_kr_dota_login
where ds = '2016-07-18' and substring(dteventtime, 1, 10) between '2016-02-01' and '2016-02-28';



insert overwrite table db_game_kr_dota.gaea_kr_dota_user_create partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,vloginip,vuin,vdevid,iisnewuin,substring(dteventtime,1 ,10),serverid
from db_game_origin_mysql_backup.gaea_kr_dota_user_create
where ds = '2016-07-18' and substring(dteventtime, 1, 10) between '2016-02-01' and '2016-02-28';


---- from db_tmp  history backup
insert overwrite table db_game_kr_dota.gaea_kr_dota_login partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,vloginip,ilevel,icharge,vfrom,vuin,substring(dteventtime, 1, 10), serverid
from db_tmp.login_dota_kr; 


insert overwrite table db_game_kr_dota.gaea_kr_dota_user_create partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,vloginip,vuin,vdevid,iisnewuin,substring(dteventtime, 1, 10), serverid
from db_tmp.user_create_dota_kr;




