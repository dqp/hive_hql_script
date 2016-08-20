set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions = 100000;

insert overwrite table db_game_kr_dota.gaea_kr_dota_diamond_flow partition(ds, serverid)
select ieventid, dteventtime, iuserid, vusername, ilevel, icharge, iaction, iflow, idiamondbefore, idiamondafter, substring(dteventtime,1 ,10), serverid from db_tmp.diamond_flow_dota_kr;

－－－ jp_dota done
insert overwrite table db_game_jp_dota.gaea_jp_dota_diamond_flow partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,ilevel,icharge,iaction,iflow,idiamondbefore,idiamondafter,substring(dteventtime, 1, 10),serverid
from db_game_jp_dota.gaea_jp_dota_diamond_flow
where ds = '2016-07-17' and substring(dteventtime, 1, 10) <= '2016-06-30';


--- jp_dota done
insert overwrite table db_game_jp_dota.gaea_jp_dota_login partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,vloginip,ilevel,icharge,vfrom,vuin,substring(dteventtime,1 ,10),serverid
from db_game_origin_mysql_backup.gaea_jp_dota_login
where ds = '2016-07-17' and substring(dteventtime, 1, 10) between '2016-07-01' and '2016-07-16';


--- jp_dota done
insert overwrite table db_game_jp_dota.gaea_jp_dota_user_create partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,vloginip,vuin,vdevid,iisnewuin,substring(dteventtime,1 ,10),serverid
from db_game_jp_dota.gaea_jp_dota_user_create
where ds = '2016-07-17' and substring(dteventtime, 1, 10) <= '2016-06-30';




－－－ kr_dota done
insert overwrite table db_game_kr_dota.gaea_kr_dota_diamond_flow partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,ilevel,icharge,iaction,iflow,idiamondbefore,idiamondafter,substring(dteventtime, 1, 10),serverid
from db_game_kr_dota.gaea_kr_dota_diamond_flow
where ds = '2016-07-18' and substring(dteventtime, 1, 10) <= '2016-06-30';



--- kr_dota done
insert overwrite table db_game_kr_dota.gaea_kr_dota_login partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,vloginip,ilevel,icharge,vfrom,vuin,substring(dteventtime,1 ,10),serverid
from db_game_kr_dota.gaea_kr_dota_login
where ds = '2016-07-18' and substring(dteventtime, 1, 10) <= '2016-06-30';



--- kr_dota done
insert overwrite table db_game_kr_dota.gaea_kr_dota_user_create partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,vloginip,vuin,vdevid,iisnewuin,substring(dteventtime,1 ,10),serverid
from db_game_kr_dota.gaea_kr_dota_user_create
where ds = '2016-07-18' and substring(dteventtime, 1, 10) <= '2016-06-30';


======from db_tmp
insert overwrite table db_game_kr_dota.gaea_kr_dota_login partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,vloginip,ilevel,icharge,vfrom,vuin,substring(dteventtime, 1, 10), serverid
from db_tmp.login_dota_kr; 


insert overwrite table db_game_kr_dota.gaea_kr_dota_user_create partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,vloginip,vuin,vdevid,iisnewuin,substring(dteventtime, 1, 10), serverid
from db_tmp.user_create_dota_kr;




－－－ kr_dota 
insert overwrite table db_game_kr_dota.gaea_kr_dota_diamond_flow partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,ilevel,icharge,iaction,iflow,idiamondbefore,idiamondafter,substring(dteventtime, 1, 10),serverid
from db_game_origin_mysql_backup.gaea_kr_dota_diamond_flow_backup
where ds = '2016-07-19' and (substring(dteventtime, 1, 10) between '2016-02-01' and '2016-02-28');



--- kr_dota 
insert overwrite table db_game_kr_dota.gaea_kr_dota_login partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,vloginip,ilevel,icharge,vfrom,vuin,substring(dteventtime,1 ,10),serverid
from db_game_origin_mysql_backup.gaea_kr_dota_login_backup
where ds = '2016-07-19' and (substring(dteventtime, 1, 10) between '2016-02-01' and '2016-02-28');



--- kr_dota 
insert overwrite table db_game_kr_dota.gaea_kr_dota_user_create partition(ds, serverid)
select ieventid,dteventtime,iuserid,vusername,vloginip,vuin,vdevid,iisnewuin,substring(dteventtime,1 ,10),serverid
from db_game_origin_mysql_backup.gaea_kr_dota_user_create_backup
where ds = '2016-07-19' and (substring(dteventtime, 1, 10) between '2016-02-01' and '2016-02-28');




--- cn_fff charge_log
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=200000;
set hive.exec.max.dynamic.partitions = 200000;

insert overwrite table db_game_cn_fff.gaea_cn_fff_data_charge_log partition(ds, serverid)
select order_channel,orderid_channel,channelaccount,date_time,productid,last_login_time,result,pay_money_fen,normal_addcredit,add_credit,keep_time_sec,sumid,substring(date_time,1,10),serverid
from db_game_origin_mysql_backup.gaea_cn_fff_data_charge_log
where ds = '2016-07-26';



insert into table db_game_cn_fff.gaea_cn_fff_data_charge_log partition(ds, serverid)
select order_channel,orderid_channel,channelaccount,date_time,productid,last_login_time,result,pay_money_fen,normal_addcredit,add_credit,keep_time_sec,sumid,substring(date_time,1,10),serverid
from db_game_origin_mysql_backup.gaea_cn_fff_data_charge_log
where ds = '2016-07-26';
