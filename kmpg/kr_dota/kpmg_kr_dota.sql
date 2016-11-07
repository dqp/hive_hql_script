---- Gboss 游戏ID标识： '1020006','1010006','1010009','1020009'
---- 每月用户USER注册数
select substring(ds, 1, 7), count(distinct split(vuin, '-')[size(split(vuin, '-')) - 1])
from db_game_kr_dota.gaea_kr_dota_user_create
where ds <= '2016-06-30'
group by substring(ds, 1, 7);


---- 每月用户USER活跃数
select substring(ds, 1, 7), count(distinct split(vuin, '-')[size(split(vuin, '-')) - 1])
from db_game_kr_dota.gaea_kr_dota_login
where ds <= '2016-06-30'
group by substring(ds, 1, 7);

--- 修正方法
select substring(t1.ds, 1, 7), count(distinct t2.vuin)
from
(
    select ds, iuserid, serverid
    from db_game_kr_dota.gaea_kr_dota_login
    where substring(ds, 1, 7) <= '2016-06' 
    group by ds, iuserid, serverid 
) t1
join
(
    select split(vuin, '-')[size(split(vuin, '-')) - 1] as vuin, iuserid, serverid
    from db_game_kr_dota.gaea_kr_dota_user_create
    where substring(ds, 1, 7) <= '2016-06'
) t2
on(t1.serverid = t2.serverid and t1.iuserid = t2.iuserid)
group by substring(t1.ds, 1, 7);



---- 每月用户USER每月留存
select substring(t1.ds, 1, 7), datediff(t2.ds, t1.ds), count(t2.vuin)
from
(
    select ds, split(vuin, '-')[size(split(vuin, '-')) - 1] as vuin
    from db_game_kr_dota.gaea_kr_dota_user_create
    where ds <= '2016-06-30'
) t1
join
(
    select i2.ds, i1.vuin
    from
    (
        select split(vuin, '-')[size(split(vuin, '-')) - 1] as vuin, iuserid, serverid
        from db_game_kr_dota.gaea_kr_dota_user_create
        where ds <= '2016-06-30'
    ) i1
    join
    (
        select ds, iuserid, serverid
        from db_game_kr_dota.gaea_kr_dota_login
        where ds <= '2016-06-30'
        group by ds, iuserid, serverid
    ) i2
    on(i1.serverid = i2.serverid and i1.iuserid = i2.iuserid)
    group by i2.ds, i1.vuin
) t2
on (t1.vuin = t2.vuin)
where datediff(t2.ds, t1.ds) in (1, 6, 13)
group by substring(t1.ds, 1, 7), datediff(t2.ds, t1.ds);


---- 重要玩家
insert overwrite table kp_gaea_audit.kr_dota_key_users
select gboss.user_id, sum(gboss.pay_amount * x.rate) from
(
    select pay_currency, pay_amount, user_id
    from db_billing.gboss_pay_orders
    where region = 'kr' 
        and pay_state = '2'
        and from_unixtime(cast(pay_time as bigint), 'yyyyMM') <= '201606'
        and product_id in ('1020006','1010006','1010009','1020009')
) gboss
join
(
    select * 
    from currency_exchange_rate
) x
on (gboss.pay_currency = x.from_currency)
group by gboss.user_id;


---- 重要玩家表创建
CREATE TABLE `kr_dota_key_users`(
    `user_id` string,
    `pay_cny` string
);



---- 元宝消耗
select substring(ds, 1, 7), iaction, sum(cast(idiamondbefore as bigint)- cast(idiamondafter as bigint))
from db_game_kr_dota.gaea_kr_dota_diamond_flow
where ds <= '2016-06-30' and lower(iflow) = 'out'
group by substring(ds, 1, 7), iaction;


---- 付费每月USER用户数
select from_unixtime(cast(gboss.pay_time as bigint), 'yyyyMM'), count(distinct gboss.user_id), sum(gboss.pay_amount * x.rate)
from
(
    select user_id, pay_time, pay_currency, pay_amount
    from db_billing.gboss_pay_orders
    where region = 'kr' 
        and pay_state = '2'
        and from_unixtime(cast(pay_time as bigint), 'yyyyMM') <= '201606'
        and product_id in ('1020006','1010006','1010009','1020009')
) gboss
join
(
    select * 
    from kp_gaea_audit.currency_exchange_rate
) x
on (gboss.pay_currency = x.from_currency)
group by from_unixtime(cast(gboss.pay_time as bigint), 'yyyyMM');


---- 重要玩家注册时间
select t1.user_id, t2.dteventtime, t1.pay_cny
from
(
    select user_id, pay_cny
    from kp_gaea_audit.kr_dota_key_users
    where cast(pay_cny as double) >= 5000
) t1
left outer join
(
    select dteventtime, split(vuin, '-')[size(split(vuin, '-')) - 1] as vuin
    from db_game_kr_dota.gaea_kr_dota_user_create
    where ds <= '2016-06-30'
    group by dteventtime, split(vuin, '-')[size(split(vuin, '-')) - 1]
) t2
on (t1.user_id = t2.vuin);
select vuinlist.vuin, coalesce(ifalist.user_id, udidlist.user_id, maclist.user_id) as userid_set
from
(
  select regexp_replace(lower(vuin),':','') as vuin, dteventtime, iuserid, serverid
  from db_game_kr_dota.gaea_kr_dota_user_create
  where ds between '2014-12-04' and '2014-12-17'
    and vuin is not null
    and vuin not in ('',' ','0','00000000-0000-0000-0000-000000000000','000000000000000','020000000000','02:00:00:00:00:00','strangedevice','visitor')
    and size(split(vuin,'-')) = 1
) vuinlist
left outer join
(
  select user_id, substring(lower(ifa),1,36) as deviceid
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
  select user_id, regexp_replace(substring(lower(mac),1,17),':','') as deviceid
  from db_billing.gboss_users_slave
  where ds='20160714'
    and region='kr'
    and ((length(mac) = 17 and size(split(lower(mac),':')) = 6 and mac != '02:00:00:00:00:00')
    or (length(mac) in (12,14,15) and mac != '020000000000'))
) maclist
on (vuinlist.vuin = maclist.deviceid)
where (ifalist.deviceid is not null
  or udidlist.deviceid is not null
  or maclist.deviceid is not null);


---- 重要玩家充值总额
select user_id, pay_cny
from kp_gaea_audit.kr_dota_key_users
where cast(pay_cny as double) >= 5000;



---- 重要玩家登活跃天数
select t1.user_id, count(distinct t2.ds), t1.pay_cny
from
(
  select user_id, pay_cny
  from kp_gaea_audit.kr_dota_key_users
  where cast(pay_cny as double) >= 5000
) t1
left outer join
(
  select i2.ds, i1.vuin
  from
  (
    select split(vuin, '-')[size(split(vuin, '-')) - 1] as vuin, iuserid, serverid
    from db_game_kr_dota.gaea_kr_dota_user_create
    where ds <= '2016-06-30'
  ) i1
  join
  (
    select ds, iuserid, serverid
    from db_game_kr_dota.gaea_kr_dota_login
    where ds <= '2016-06-30'
    group by ds, iuserid, serverid
  ) i2
  on(i1.serverid = i2.serverid and i1.iuserid = i2.iuserid)
  group by i2.ds, i1.vuin
) t2
on (t1.user_id = t2.vuin)
group by t1.user_id, t1.pay_cny;



---- 重要玩家活跃天数
hive -e "
select big.user_id, big.pay_cny, count(distinct active.ds)
from
(
  select user_id, pay_cny
  from kp_gaea_audit.kr_dota_key_users
  where cast(pay_cny as double) >= 5000
) big
left outer join
(
  select i1.ds, i2.user_id
  from
  (
    select ds, iuserid as roleid, serverid
    from db_game_kr_dota.gaea_kr_dota_login
    where ds <= '2016-06-30'
    group by ds, iuserid, serverid
  ) i1
  left outer join
  (
    select iuserid as roleid, iserverid, serverid, split(vdeviceid, '-')[1] as user_id
    from db_game_kr_dota.gaea_kr_dota_user
    where ds = '2016-08-02' and size(split(vdeviceid, '-')) = 2
  ) i2
  on (i1.serverid in (i2.serverid, i2.iserverid) and i1.roleid = i2.roleid)
  group by i1.ds, i2.user_id
) active
on (big.user_id = active.user_id)
group by big.user_id, big.pay_cny;
" > kr_dota_key_users_days1.tsv


---- 重要玩家钻石消耗
select t2.iaction, sum(t2.yuanbao)
from
(
  select user_id, pay_cny
  from kp_gaea_audit.kr_dota_key_users
  where cast(pay_cny as double) >= 5000
) t1
left outer join
(
  select i1.m, i2.vuin, i1.iaction, sum(i1.yuanbao) as yuanbao
  from
  (
    select substring(ds, 1, 7) as m, serverid, iuserid, iaction, sum(cast(idiamondbefore as bigint)- cast(idiamondafter as bigint)) as yuanbao
    from db_game_kr_dota.gaea_kr_dota_diamond_flow
    where ds <= '2016-06-30' and lower(iflow) = 'out'
    group by substring(ds, 1, 7), serverid, iuserid, iaction
  ) i1
  join
  (
    select split(vuin, '-')[size(split(vuin, '-')) - 1] as vuin, serverid, iuserid
    from db_game_kr_dota.gaea_kr_dota_user_create
    where ds <= '2016-06-30'
    group by split(vuin, '-')[size(split(vuin, '-')) - 1], serverid, iuserid
  ) i2
  on (i1.serverid = i2.serverid and i1.iuserid = i2.iuserid)
  group by i1.m, i2.vuin, i1.iaction
) t2
on (t1.user_id = t2.vuin)
group by t2.iaction;


---- 重要玩家钻石消耗-明细
select t1.user_id, t1.pay_cny, t2.iaction, sum(t2.yuanbao)
from
(
  select user_id, pay_cny
  from kp_gaea_audit.kr_dota_key_users
  where cast(pay_cny as double) >= 5000
) t1
left outer join
(
  select i1.m, i2.vuin, i1.iaction, sum(i1.yuanbao) as yuanbao
  from
  (
    select substring(ds, 1, 7) as m, serverid, iuserid, iaction, sum(cast(idiamondbefore as bigint)- cast(idiamondafter as bigint)) as yuanbao
    from db_game_kr_dota.gaea_kr_dota_diamond_flow
    where ds <= '2016-06-30' and lower(iflow) = 'out'
    group by substring(ds, 1, 7), serverid, iuserid, iaction
  ) i1
  join
  (
    select split(vuin, '-')[size(split(vuin, '-')) - 1] as vuin, serverid, iuserid
    from db_game_kr_dota.gaea_kr_dota_user_create
    where ds <= '2016-06-30'
    group by split(vuin, '-')[size(split(vuin, '-')) - 1], serverid, iuserid
  ) i2
  on (i1.serverid = i2.serverid and i1.iuserid = i2.iuserid)
  group by i1.m, i2.vuin, i1.iaction
) t2
on (t1.user_id = t2.vuin)
group by t1.user_id, t1.pay_cny, t2.iaction;



---- 重要玩家充值获得
select t1.user_id, t1.pay_cny, t2.iaction, sum(t2.yuanbao)
from
(
  select user_id, pay_cny
  from kp_gaea_audit.kr_dota_key_users
  where cast(pay_cny as double) >= 5000
) t1
left outer join
(
  select i1.m, i2.vuin, i1.iaction, sum(i1.yuanbao) as yuanbao
  from
  (
    select substring(ds, 1, 7) as m, serverid, iuserid, iaction, sum(cast(idiamondbefore as bigint)- cast(idiamondafter as bigint)) as yuanbao
    from db_game_kr_dota.gaea_kr_dota_diamond_flow
    where ds <= '2016-06-30' and lower(iflow) = 'in' and iaction = '18'
    group by substring(ds, 1, 7), serverid, iuserid, iaction
  ) i1
  join
  (
    select split(vuin, '-')[size(split(vuin, '-')) - 1] as vuin, serverid, iuserid
    from db_game_kr_dota.gaea_kr_dota_user_create
    where ds <= '2016-06-30'
    group by split(vuin, '-')[size(split(vuin, '-')) - 1], serverid, iuserid
  ) i2
  on (i1.serverid = i2.serverid and i1.iuserid = i2.iuserid)
  group by i1.m, i2.vuin, i1.iaction
) t2
on (t1.user_id = t2.vuin)
group by t1.user_id, t1.pay_cny, t2.iaction;



---- 重要玩家截止时间钻石结余
select t1.user_id, t1.pay_cny, sum(t2.idiamondafter)
from
(
    select user_id, pay_cny
    from kp_gaea_audit.kr_dota_key_users
    where cast(pay_cny as double) >= 5000
) t1
left outer join
(
    select i3.vuin, sum(i2.idiamondafter) as idiamondafter
    from 
    (
        select iuserid, serverid, max(dteventtime) as dteventtime
        from db_game_kr_dota.gaea_kr_dota_diamond_flow
        where ds <= '2016-06-30'
        group by iuserid, serverid
    ) i1
    join
    (
        select iuserid, serverid, dteventtime, idiamondafter
        from db_game_kr_dota.gaea_kr_dota_diamond_flow
        where ds <= '2016-06-30'
    ) i2
    on(i1.serverid = i2.serverid and i1.iuserid = i2.iuserid and i1.dteventtime = i2.dteventtime)
    join
    (
    select split(vuin, '-')[size(split(vuin, '-')) - 1] as vuin, serverid, iuserid
    from db_game_kr_dota.gaea_kr_dota_user_create
    where ds <= '2016-06-30'
    group by split(vuin, '-')[size(split(vuin, '-')) - 1], serverid, iuserid
    ) i3
    on (i2.serverid = i3.serverid and i2.iuserid = i3.iuserid)
    group by i3.vuin
) t2
on (t1.user_id = t2.vuin)
group by t1.user_id, t1.pay_cny;