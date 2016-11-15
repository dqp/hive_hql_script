---- Gboss 游戏ID标识： '1020006','1010006','1010009','1020009'
---- 1020006 安卓,1010006 iOS, 1010009 kakao, 1020009 Kakao_android
---- 每月用户USER注册数

--临时表 (通过设备号关联分服角色与用户，带注册日)
create table db_game_kr_dota.gaea_kr_dota_user_role_server_tmp
(
    dteventtime string,
    userid string,
    roleid string,
    serverid string,
    vdevid string,
    tag string
);

insert into table db_game_kr_dota.gaea_kr_dota_user_role_server_tmp
select dset.dteventtime, dset.userid, dset.roleid, dset.serverid, dset.vdevid, dset.tag
from
(
    select dteventtime, split(vuin, '-')[size(split(vuin, '-')) - 1] as userid, iuserid as roleid, serverid, regexp_replace(lower(vdevid),':','') as vdevid, 'uc' as tag
    from db_game_kr_dota.gaea_kr_dota_user_create_csv_serde
    where ((ds between '2014-12-04' and '2014-12-17') and (size(split(vuin,'-')) = 2 or substring(vuin,1,4) = 'del-'))
        or (ds between '2014-12-18' and '2016-06-30')
    group by dteventtime, split(vuin, '-')[size(split(vuin, '-')) - 1], iuserid, serverid, regexp_replace(lower(vdevid),':','')

    union all

    select '' as dteventtime, split(vdeviceid, '-')[size(split(vdeviceid, '-')) - 1] as userid, iuserid as roleid, serverid, '' as vdevid, 'u' as tag
    from db_game_kr_dota.gaea_kr_dota_user
    where (size(split(vdeviceid,'-')) = 2 or substring(vdeviceid,1,4) = 'del-')
    group by split(vdeviceid, '-')[size(split(vdeviceid, '-')) - 1], iuserid, serverid
                
    union all
    
    select from_unixtime(o1.k,'yyyy-MM-dd HH:mm:ss') as dteventtime, o1.userid as userid, o1.roleid as roleid, o1.serverid as serverid, o1.vdevid as vdevid, o1.tag as tag
    from
    (
        select min(unix_timestamp(dteventtime,'yyyy-MM-dd HH:mm:ss')) as k, split(vuin, '-')[size(split(vuin, '-')) - 1] as userid, iuserid as roleid, serverid, '' as vdevid, 'l' as tag
        from db_game_kr_dota.gaea_kr_dota_login_csv_serde
        where ds != '2016-07-16' 
            and ds != '2016-07-18'
            and (
                (size(split(vuin,'-')) = 2 
                or substring(vuin,1,4) = 'del-') 
                and vuin is not null 
                and vuin != 'NULL'
            )
        group by split(vuin, '-')[size(split(vuin, '-')) - 1], iuserid, serverid
    ) o1
    
    union all
    
    select from_unixtime(cast(reg_time as bigint),'yyyy-MM-dd HH:mm:ss') as dteventtime, user_id as userid, '' as roleid, '' as serverid, '' as vdevid, 'gu' as tag
    from db_billing.gboss_users_slave
    where ds='20160714'
        and region='kr'
        and user_id is not null
        and lower(user_id) != 'null'
    group by reg_time, user_id

    union all

    select vuinlist.dteventtime as dteventtime, coalesce(ifalist.user_id, udidlist.user_id, maclist.user_id) as userid, vuinlist.iuserid as roleid, vuinlist.serverid as serverid, vuinlist.vuin as vdevid, concat(vuinlist.tag0,'m') as tag
    from
    (
        select dteventtime, regexp_replace(lower(vuin),':','') as vuin, iuserid, serverid, 'uc' as tag0
        from db_game_kr_dota.gaea_kr_dota_user_create_csv_serde
        where ds between '2014-12-04' and '2014-12-17'
            and vuin is not null
            and vuin not in ('',' ','0','00000000-0000-0000-0000-000000000000','000000000000000','020000000000','02:00:00:00:00:00','strangedevice','visitor')
            and (size(split(vuin,'-')) != 2
            and substring(vuin,1,4) != 'del-')
            
        union all
        
        select '' as dteventtime, regexp_replace(lower(vdeviceid),':','') as vuin, iuserid, serverid, 'u' as tag0
        from db_game_kr_dota.gaea_kr_dota_user
        where (size(split(vdeviceid,'-')) != 2
            and substring(vdeviceid,1,4) != 'del-')
            and vdeviceid is not null
            and vdeviceid not in ('',' ','0','00000000-0000-0000-0000-000000000000','000000000000000','020000000000','02:00:00:00:00:00','strangedevice','visitor')
        
    ) vuinlist
    left outer join
    (
        select user_id, lower(ifa) as deviceid
        from db_billing.gboss_users_slave
        where ds='20160714'
            and region='kr'
            and ifa is not null
            and ifa not in ('',' ','0','00000000-0000-0000-0000-000000000000','000000000000000','020000000000','02:00:00:00:00:00','strangedevice','visitor')
            and regexp_replace(ifa,'[0]{1,100}','') != ''
    ) ifalist
    on (vuinlist.vuin = ifalist.deviceid)
    left outer join
    (
        select user_id, lower(udid) as deviceid
        from db_billing.gboss_users_slave
        where ds='20160714'
            and region='kr'
            and udid is not null
            and udid not in ('',' ','0','00000000-0000-0000-0000-000000000000','000000000000000','020000000000','02:00:00:00:00:00','strangedevice','visitor')
            and regexp_replace(udid,'[0]{1,100}','') != ''
    ) udidlist
    on (vuinlist.vuin = udidlist.deviceid)
    left outer join
    (
        select user_id, regexp_replace(lower(mac),':','') as deviceid
        from db_billing.gboss_users_slave
        where ds='20160714'
            and region='kr'
            and ((length(mac) = 17 and size(split(lower(mac),':')) = 6 and mac != '02:00:00:00:00:00')
            or (length(mac) in (12,14,15) and mac != '020000000000'))
    ) maclist
    on (vuinlist.vuin = maclist.deviceid)
    where (ifalist.deviceid is not null
        or udidlist.deviceid is not null
        or maclist.deviceid is not null)
    group by dteventtime, coalesce(ifalist.user_id, udidlist.user_id, maclist.user_id), vuinlist.iuserid, vuinlist.serverid, vuinlist.vuin, concat(vuinlist.tag0,'m')
) dset
group by dset.dteventtime, dset.userid, dset.roleid, dset.serverid, dset.vdevid, dset.tag;


---- 注册时间
select keyuser.user_id, usertmp.dteventtime, keyuser.pay_cny
from
(
    select user_id, pay_cny
    from kp_gaea_audit.kr_dota_key_users
    where cast(pay_cny as double) >= 5000
) keyuser
left outer join
(
    select t.userid as userid, from_unixtime(t.dt,'yyyy-MM-dd HH:mm:ss') as dteventtime
    from
    (
        select userid, min(unix_timestamp(dteventtime,'yyyy-MM-dd HH:mm:ss')) as dt
        from db_game_kr_dota.gaea_kr_dota_user_role_server_tmp
        where dteventtime is not null
            and lower(dteventtime) != 'null'
        group by userid
    ) t
) usertmp
on (keyuser.user_id = usertmp.userid);



----- 注册时间
select keyuser.user_id, keyuser.pay_cny, login.firstdate
from
(
    select user_id, pay_cny
    from kp_gaea_audit.kr_dota_key_users
    where cast(pay_cny as double) >= 5000
) keyuser
left outer join
(
    select user_id, min(ldate) firstdate
    from db_billing.gboss_user_login_log
    group by user_id
) login
on (keyuser.user_id = login.user_id)
group by keyuser.user_id, keyuser.pay_cny;



----- 活跃天数
select keyuser.user_id, keyuser.pay_cny, count(distinct login.ldate)
from
(
    select user_id, pay_cny
    from kp_gaea_audit.kr_dota_key_users
    where cast(pay_cny as double) >= 5000
) keyuser
left outer join
(
    select user_id, ldate
    from db_billing.gboss_user_login_log
    group by user_id, ldate
) login
on (keyuser.user_id = login.user_id)
group by keyuser.user_id, keyuser.pay_cny;


--活跃天数 添加列[注册至2016-06-30天数]
select t1.user_id, t1.pay_cny, max(t2.daysince), count(distinct t2.ds)
from
(
    select user_id, pay_cny
    from kp_gaea_audit.kr_dota_key_users
    where cast(pay_cny as double) >= 5000
) t1
left outer join
(
    select i2.ds, i1.userid, i1.daysince
    from
    (
        select userid, serverid, roleid, datediff('2016-06-30',from_unixtime(unix_timestamp(dteventtime,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd'))+1 as daysince
        from db_game_kr_dota.gaea_kr_dota_user_role_server_tmp
        group by userid, serverid, roleid, datediff('2016-06-30',from_unixtime(unix_timestamp(dteventtime,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd'))+1
    ) i1
    left outer join
    (
        select ds, iuserid as roleid, serverid
        from db_game_kr_dota.gaea_kr_dota_login_csv_serde
        where ds <= '2016-06-30'
        group by ds, iuserid, serverid
    ) i2
    on (i1.serverid = i2.serverid and i1.roleid = i2.roleid)
    group by i2.ds, i1.userid, i1.daysince
    having i2.ds is not null
        and lower(i2.ds) != 'null'
) t2
on (t1.user_id = t2.userid)
group by t1.user_id, t1.pay_cny;
