---- 自由之战user每月新增
select month, count(distinct accountid)
from (
    select t1.accountid, min(substring(t2.ds, 1, 6)) as month
    from
    (
        select id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        where ds = '2016-07-17'
        group by id, accountid, serverid
    ) t1
    join
    (
        select server_id, role_id, min(ds) ds
        from db_stat_platform.gaea_stat_role_login
        where ds <='20160630'
        group by server_id, role_id
    ) t2
    on (t1.id = t2.role_id and t1.serverid = t2.server_id)
    group by t1.accountid
) a
group by month;


-----自由之战每月活跃数
select t2.month, count(distinct t1.accountid)
from
(
    select id, accountid, serverid
    from db_game_cn_fff.gaea_cn_fff_data_summoner
    where ds = '2016-07-17'
    group by id, accountid, serverid
) t1
join
(
    select server_id, role_id, substring(ds, 1, 6) as month
    from db_stat_platform.gaea_stat_role_login
    where ds <='20160630'
    group by server_id, role_id, substring(ds, 1, 6)
) t2
on (t1.id = t2.role_id and t1.serverid = t2.server_id)
group by t2.month;



-----自由之战钻石消耗(event_type>4000 and event_type<5000 钻石消耗)
select substring(ds, 1, 6), event_type, sum(cast(ingot_num as bigint))
from db_stat_platform.gaea_stat_currency_track
where ds <= '20160630' and (cast(event_type as bigint) between 4001 and 5000)
group by substring(ds, 1, 6), event_type;


-----自由之战钻石获得(event_type>3000 and event_type<4000 钻石消耗)
select substring(ds, 1, 6), event_type, sum(cast(ingot_num as bigint))
from db_stat_platform.gaea_stat_currency_track
where ds <= '20160630' and (cast(event_type as bigint) between 3001 and 4000)
group by substring(ds, 1, 6), event_type;


---- 自由之战每月留存
select substring(n.ds, 1, 6), datediff(from_unixtime(unix_timestamp(a.ds, 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(unix_timestamp(n.ds, 'yyyyMMdd'), 'yyyy-MM-dd')), count(distinct a.accountid)
from
(
    select t1.accountid, min(t2.ds) as ds
    from
    (
        select id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        where ds = '2016-07-17'
        group by id, accountid, serverid
    ) t1
    join
    (
        select server_id, role_id, min(ds) ds
        from db_stat_platform.gaea_stat_role_login
        where ds <='20160630'
        group by server_id, role_id
    ) t2
    on (t1.id = t2.role_id and t1.serverid = t2.server_id)
    group by t1.accountid
) n
join
(
    select t2.ds, t1.accountid
    from
    (
        select id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        where ds = '2016-07-17'
        group by id, accountid, serverid
    ) t1
    join
    (
        select server_id, role_id, ds
        from db_stat_platform.gaea_stat_role_login
        where ds <='20160630'
        group by server_id, role_id, ds
    ) t2
    on (t1.id = t2.role_id and t1.serverid = t2.server_id)
) a
on(n.accountid = a.accountid)
where datediff(from_unixtime(unix_timestamp(a.ds, 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(unix_timestamp(n.ds, 'yyyyMMdd'), 'yyyy-MM-dd')) in ('1', '6', '13')
group by substring(n.ds, 1, 6), datediff(from_unixtime(unix_timestamp(a.ds, 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(unix_timestamp(n.ds, 'yyyyMMdd'), 'yyyy-MM-dd'));


--- 自由之战付费玩家
insert overwrite table kp_gaea_audit.cn_fff_users_payment
select 'gaea', user_id, pay_currency, sum(pay_amount)
from db_billing.gboss_pay_orders
where region = 'cn' and product_id in ('510002', '530002', '511002', '531002', '520002') and pay_state = '2'
and from_unixtime(cast(pay_time as bigint), 'yyyyMMdd') <= '20160630'
group by 'gaea', user_id, pay_currency
union all
select i1.channel, i2.accountid, 'CNY', sum(i1.rmb) as rmb
from
(
    select serverid, sumid, split(order_channel, '_')[size(split(order_channel, '_')) - 1] as channel, sum(pay_money_fen)/100.0 as rmb
    from db_game_cn_fff.gaea_cn_fff_data_charge_log
    where ds <='2016-06-30'
    and split(order_channel, '_')[size(split(order_channel, '_')) - 1] != '1009' -- App Store
    and split(order_channel, '_')[size(split(order_channel, '_')) - 1] != '1010' -- GAEA 越狱
    and split(order_channel, '_')[size(split(order_channel, '_')) - 1] != '1' -- 测试
    and split(order_channel, '_')[size(split(order_channel, '_')) - 1] != '1142' -- 自营:安卓国际包
    group by serverid, sumid, split(order_channel, '_')[size(split(order_channel, '_')) - 1]
) i1
left outer join
(
    select id, accountid, serverid
    from db_game_cn_fff.gaea_cn_fff_data_summoner
    where ds = '2016-07-17'
    group by id, accountid, serverid
) i2
on(i1.serverid = i2.serverid and i1.sumid = i2.id)
group by i1.channel, i2.accountid, 'CNY';





select '201605' as month, from_unixtime(unix_timestamp(trackdata.ds,'yyyy-MM-dd'),'yyyyMMdd'),
    trackdata.reason, trackdata.reason_name, sum(trackdata.uses), sum(trackdata.vcurrency)
from
(
    select userid, min(ds) as ds
    from db_game_g_kom.kom_gem_transactions
    where ds between '2016-01-01' and '2016-05-31'
        and fullfilled = '1'
        and serverid != '150'
        and transactiontype = '1'
    group by userid
) chargedata
join
(
    select userid, sum(proposedamount) as vcurrency, count(userid) as uses, transactiontype as reason, transactiontype as reason_name, ds
    from db_game_g_kom.kom_gem_transactions
    where (ds between '2016-05-01' and '2016-05-31')
        and fullfilled = '1'
        and serverid != '150'
        and proposedamount != 0
    group by userid, transactiontype, ds
) trackdata
on (chargedata.userid = trackdata.userid)
where datediff(trackdata.ds, chargedata.ds) >= 0
group by trackdata.reason, trackdata.reason_name, trackdata.ds;




