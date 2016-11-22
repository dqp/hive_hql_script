
--临时表db_game_kr_dota.gaea_kr_dota_user_role_server_tmp 根据设备号，将<roleid, serverid>和userid对应起来
--tag表示对应的来源，从user_create表数据、user表数据、login表数据直接来的分别标识为uc,u,l
--通过匹配得来加上m(match)
--加上时间dteventtime用来得到注册时间
--通过接合的uc u两个表中的设备号，和gboss表里的设备号(idfa, udid, mac)进行匹配，找到关联
--含vusername字段的表都需要采用_csv_serde

---- Gboss 游戏ID标识： '1020006','1010006','1010009','1020009'
---- 1020006 安卓,1010006 iOS, 1010009 kakao, 1020009 Kakao_android

-- 20160630以前用户----> db_game_kr_dota.gaea_kr_dota_user_role_server_tmp

-临时表 (通过设备号关联分服角色与用户，带注册日)
create table db_game_kr_dota.gaea_kr_dota_user_role_server_tmp
(
    dteventtime string,
    userid string,
    roleid string,
    serverid string,
    vdevid string,
    tag string
);

create table db_game_kr_dota.gaea_kr_dota_user_role_server_map
(
    userid string,
    roleid string,
    serverid string
)
STORED AS PARQUET;









insert overwrite table db_game_kr_dota.gaea_kr_dota_user_role_server_map
select dset.userid, dset.roleid, dset.serverid
from
(
    ---- 创建角色的时候记录正确的映射关系
    select split(vuin, '-')[size(split(vuin, '-')) - 1] as userid, iuserid as roleid, serverid
    from db_game_kr_dota.gaea_kr_dota_user_create
    where (ds between '2014-12-04' and '2014-12-17' and size(split(vuin,'-')) = 2 and substring(vuin,1,4) != 'del-')
        or (ds between '2014-12-18' and '2016-09-30')
    group by split(vuin, '-')[size(split(vuin, '-')) - 1], iuserid, serverid

    union all
    ---- 通过角色状态表查找映射关系
    select split(vdeviceid, '-')[size(split(vdeviceid, '-')) - 1] as userid, iuserid as roleid, iserverid as serverid
    from db_game_kr_dota.gaea_kr_dota_user
    where size(split(vdeviceid,'-')) = 2 
        and substring(vdeviceid, 1, 4) != 'del-'
    group by split(vdeviceid, '-')[size(split(vdeviceid, '-')) - 1], iuserid, iserverid
                
    union all

    ---- 通过登录日志查找映射关系
    select split(vuin, '-')[size(split(vuin, '-')) - 1] as userid, iuserid as roleid, serverid
    from db_game_kr_dota.gaea_kr_dota_login
    where ds between '2014-12-18' and '2016-09-30'
        and size(split(vuin,'-')) = 2 
        and substring(vuin, 1, 4) != 'del-'
        and vuin is not null 
        and vuin != 'NULL'
    group by split(vuin, '-')[size(split(vuin, '-')) - 1], iuserid, serverid

    union all

    select coalesce(ifalist.user_id, udidlist.user_id, maclist.user_id) as userid, vuinlist.iuserid as roleid, vuinlist.serverid as serverid
    from
    (
        select dteventtime, regexp_replace(lower(vuin),':','') as vuin, iuserid, serverid, 'uc' as tag0
        from db_game_kr_dota.gaea_kr_dota_user_create
        where ds between '2014-12-04' and '2014-12-17'
            and vuin is not null
            and vuin not in ('',' ','0','00000000-0000-0000-0000-000000000000','000000000000000','020000000000','02:00:00:00:00:00','strangedevice','visitor')
            and (size(split(vuin,'-')) != 2
            and substring(vuin,1,4) != 'del-')  ---- invalid
    ) vuinlist
    left outer join
    (
        select user_id, lower(ifa) as deviceid
        from db_billing.gboss_users_slave
        where region='kr'
            and ifa is not null
            and ifa not in ('',' ','0','00000000-0000-0000-0000-000000000000','000000000000000','020000000000','02:00:00:00:00:00','strangedevice','visitor')
            and regexp_replace(ifa,'[0]{1,100}','') != ''
    ) ifalist
    on (vuinlist.vuin = ifalist.deviceid)
    left outer join
    (
        select user_id, lower(udid) as deviceid
        from db_billing.gboss_users_slave
        where region='kr'
            and udid is not null
            and udid not in ('',' ','0','00000000-0000-0000-0000-000000000000','000000000000000','020000000000','02:00:00:00:00:00','strangedevice','visitor')
            and regexp_replace(udid,'[0]{1,100}','') != ''
    ) udidlist
    on (vuinlist.vuin = udidlist.deviceid)
    left outer join
    (
        select user_id, regexp_replace(lower(mac),':','') as deviceid
        from db_billing.gboss_users_slave
        where region='kr'
            and ((length(mac) = 17 and size(split(lower(mac),':')) = 6 and mac != '02:00:00:00:00:00')
            or (length(mac) in (12,14,15) and mac != '020000000000'))
    ) maclist
    on (vuinlist.vuin = maclist.deviceid)
    where (ifalist.deviceid is not null
        or udidlist.deviceid is not null
        or maclist.deviceid is not null)
    group by coalesce(ifalist.user_id, udidlist.user_id, maclist.user_id), vuinlist.iuserid, vuinlist.serverid
) dset
group by dset.userid, dset.roleid, dset.serverid;



1.每天新增注册玩家（个）
select regist.dt, count(distinct regist.userid) as users
from
(
  select userid, min(substring(dteventtime, 1, 10)) as dt
  from db_game_kr_dota.gaea_kr_dota_user_role_server_tmp
  where dteventtime is not null
      and lower(dteventtime) != 'null'
      and substring(dteventtime, 1, 10) <= '2016-06-30'
  group by userid
) regist
group by regist.dt
union all
select regist2.dt, count(distinct split(regist2.userid, '-')[size(split(regist2.userid, '-')) - 1]) as users
from
(
  select min(substring(ds, 1, 10)) as dt, vuin as userid
  from db_game_kr_dota.gaea_kr_dota_user_create
  where ds between '2016-07-01' and '2016-09-30'
  group by vuin
) regist2
group by regist2.dt
order by dt asc


select min(substring(ds, 1, 10)) as dt, split(vuin, '-')[size(split(vuin, '-')) - 1] as userid
from db_game_kr_dota.gaea_kr_dota_user_create
where ds between '2014-12-04' and '2016-09-30'
    and size(split(vuin,'-')) = 2
    and substring(vuin,1,4) != 'del-'
group by vuin
union
select iuc.ds, um.userid
from
(
    select ds, iuserid as roleid, serverid
    from db_game_kr_dota.gaea_kr_dota_user_create
    where ds between '2014-12-04' and '2014-12-17'
        and size(split(vuin,'-')) != 2
        and substring(vuin,1,4) != 'del-'  ---- invalid
            
) iuc
left join
(
    select userid, roleid, serverid
    from db_game_kr_dota.gaea_kr_dota_user_role_server_map
    group by userid, roleid, serverid
) um
on (iuc.serverid = um.serverid and iuc.roleid = um.roleid)
group by iuc.ds, um.userid


create table db_game_kr_dota.gaea_kr_dota_user_role_server_map
(
    dteventtime string,
    userid string,
    roleid string,
    serverid string,
    vdevid string,
    tag string
)
STORED AS PARQUET;



2.每天活跃用户数（个）
select regist.dt, count(distinct regist.userid) as users
from
(
  select userid, substring(dteventtime, 1, 10) as dt
  from db_game_kr_dota.gaea_kr_dota_user_role_server_tmp
  where dteventtime is not null
      and lower(dteventtime) != 'null'
      and substring(dteventtime, 1, 10) <= '2016-06-30'
  group by userid, substring(dteventtime, 1, 10)
) regist
group by regist.dt
union all
select regist2.dt, count(distinct split(regist2.userid, '-')[size(split(regist2.userid, '-')) - 1]) as users
from
(
  select substring(ds, 1, 10) as dt, vuin as userid
  from db_game_kr_dota.gaea_kr_dota_user_create
  where ds between '2016-07-01' and '2016-09-30'
  group by substring(ds, 1, 10), vuin
) regist2
group by regist2.dt
order by dt asc

3.每月活跃用户数（个）
select regist.month, count(distinct regist.userid) as users
from
(
  select userid, substring(dteventtime, 1, 7) as month
  from db_game_kr_dota.gaea_kr_dota_user_role_server_tmp
  where dteventtime is not null
      and lower(dteventtime) != 'null'
      and substring(dteventtime, 1, 10) <= '2016-06-30'
  group by userid, substring(dteventtime, 1, 7)
) regist
group by regist.month
union all
select regist2.month, count(distinct split(regist2.userid, '-')[size(split(regist2.userid, '-')) - 1]) as users
from
(
  select substring(ds, 1, 7) as month, vuin as userid
  from db_game_kr_dota.gaea_kr_dota_user_create
  where ds between '2016-07-01' and '2016-09-30'
  group by substring(ds, 1, 7), vuin
) regist2
group by regist2.month
order by month asc

4.每天付费用户数（个）、充值流水（元）
select from_unixtime(cast(gboss.pay_time as bigint), 'yyyy-MM-dd'), count(distinct gboss.user_id), sum(gboss.pay_amount * x.rate)
from
(
    select user_id, pay_time, pay_currency, pay_amount
    from db_billing.gboss_pay_orders
    where region = 'kr' 
        and pay_state = '2'
        and from_unixtime(cast(pay_time as bigint), 'yyyy-MM-dd') <= '2016-09-30'
        and product_id in ('1020006','1010006','1010009','1020009')
) gboss
join
(
    select * 
    from kp_gaea_audit.currency_exchange_rate
) x
on (gboss.pay_currency = x.from_currency)
group by from_unixtime(cast(gboss.pay_time as bigint), 'yyyy-MM-dd');

5.每天所有玩家充值虚拟货币数
select ds, iaction, sum(cast(idiamondafter as bigint)- cast(idiamondbefore as bigint)) as yuanbao
from db_game_kr_dota.gaea_kr_dota_diamond_flow
where ds <= '2016-09-30'
  and lower(iflow) = 'in'
  and iaction = '18'
group by ds, iaction
order by ds, iaction;

6.每天所有玩家消耗虚拟货币数
select ds, iaction, sum(cast(idiamondbefore as bigint)- cast(idiamondafter as bigint))
from db_game_kr_dota.gaea_kr_dota_diamond_flow
where ds <= '2016-09-30' 
  and lower(iflow) = 'out'
group by ds, iaction
order by ds, iaction;

7.付费每月USER用户数，分平台
select from_unixtime(cast(gboss.pay_time as bigint), 'yyyy-MM'), product_id, count(distinct gboss.user_id), sum(gboss.pay_amount * x.rate)
from
(
    select user_id, product_id, pay_time, pay_currency, pay_amount
    from db_billing.gboss_pay_orders
    where region = 'kr' 
        and pay_state = '2'
        and from_unixtime(cast(pay_time as bigint), 'yyyy-MM-dd') <= '2016-09-30'
        and product_id in ('1020006','1010006','1010009','1020009')
) gboss
join
(
    select * 
    from kp_gaea_audit.currency_exchange_rate
) x
on (gboss.pay_currency = x.from_currency)
group by from_unixtime(cast(gboss.pay_time as bigint), 'yyyy-MM'), product_id;

