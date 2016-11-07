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







