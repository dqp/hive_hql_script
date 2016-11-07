---- 自由之战官方gaea官方渠道标识： 1009(App Store) 1010(GAEA 越狱) 1142(自营:安卓国际包) 1(测试)
---- GBoss游戏标识： '510002', '530002', '511002', '531002', '520002'
---- 自由之战user每月注册用户数：月份  注册用户数
hive -e "
select month, count(distinct user_id, channel)
from 
(
    select t1.accountid as user_id, if (t2.role_channel in ('1009', '1010', '1142'), 'gaea', t2.role_channel) as channel, min(substring(t2.ds, 1, 6)) as month
    from
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t1
    join
    (
        select server_id, role_id, role_channel, min(ds) ds
        from db_stat_platform.gaea_stat_role_login
        where ds <='20160630' and role_channel != '1'
        group by server_id, role_id, role_channel
    ) t2
    on (t1.role_id = t2.role_id and t1.serverid = t2.server_id)
    group by t1.accountid, if (t2.role_channel in ('1009', '1010', '1142'), 'gaea', t2.role_channel)
) a
group by month;
" > cn_fff_register_monthly.tsv


-----自由之战每月活跃数：月份  活跃用户数
hive -e "
select t2.month, count(distinct t1.user_id, t2.channel)
from
(
    select id as role_id, accountid as user_id, serverid
    from db_game_cn_fff.gaea_cn_fff_data_summoner
    group by id, accountid, serverid
) t1
join
(
    select server_id, role_id, if (role_channel in ('1009', '1010', '1142'), 'gaea', role_channel) as channel, substring(ds, 1, 6) as month
    from db_stat_platform.gaea_stat_role_login
    where ds <='20160630' and role_channel != '1'
    group by server_id, role_id, if (role_channel in ('1009', '1010', '1142'), 'gaea', role_channel), substring(ds, 1, 6)
) t2
on (t1.role_id = t2.role_id and t1.serverid = t2.server_id)
group by t2.month;
" > cn_fff_active_monthly.tsv


-----自由之战钻石消耗(event_type>4000 and event_type<5000 钻石消耗)：月份  钻石消耗类型 钻石数量
hive -e "
select substring(ds, 1, 6), event_type, sum(cast(ingot_num as bigint))
from db_stat_platform.gaea_stat_currency_track
where ds <= '20160630' and (cast(event_type as bigint) between 4001 and 5000)
group by substring(ds, 1, 6), event_type;
" > cn_fff_yuanbao_use_monthly.tsv



-----自由之战钻石获得(event_type>3000 and event_type<4000 钻石消耗)：月份  钻石类型 钻石数量
hive -e "
select substring(ds, 1, 6), event_type, sum(cast(ingot_num as bigint))
from db_stat_platform.gaea_stat_currency_track
where ds <= '20160630' and (cast(event_type as bigint) between 3001 and 4000)
group by substring(ds, 1, 6), event_type;
" > cn_fff_yuanbao_get_monthly.tsv



---- 自由之战每月留存： 月份， 留存标识， 留存数
hive -e "
select substring(n.ds, 1, 6), datediff(from_unixtime(unix_timestamp(a.ds, 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(unix_timestamp(n.ds, 'yyyyMMdd'), 'yyyy-MM-dd')), count(distinct a.accountid)
from
(
    select t1.accountid as user_id, if (t2.role_channel in ('1009', '1010', '1142'), 'gaea', t2.role_channel) as channel, min(t2.ds) as ds
    from
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t1
    join
    (
        select server_id, role_id, role_channel, min(ds) ds
        from db_stat_platform.gaea_stat_role_login
        where ds <='20160630' and role_channel != '1'
        group by server_id, role_id, role_channel
    ) t2
    on (t1.role_id = t2.role_id and t1.serverid = t2.server_id)
    group by t1.accountid, if (t2.role_channel in ('1009', '1010', '1142'), 'gaea', t2.role_channel)
) n
join
(
    select t1.user_id, if (t2.role_channel in ('1009', '1010', '1142'), 'gaea', t2.role_channel) as channel, t2.ds
    from
    (
        select id as role_id, accountid as user_id, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t1
    join
    (
        select server_id, role_id, role_channel, ds
        from db_stat_platform.gaea_stat_role_login
        where ds <='20160630' and role_channel != '1'
        group by server_id, role_id, role_channel, ds
    ) t2
    on (t1.role_id = t2.role_id and t1.serverid = t2.server_id)
    group by t1.user_id, if (t2.role_channel in ('1009', '1010', '1142'), 'gaea', t2.role_channel), t2.ds
) a
on(n.user_id = a.user_id and n.channel = a.channel)
where datediff(from_unixtime(unix_timestamp(a.ds, 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(unix_timestamp(n.ds, 'yyyyMMdd'), 'yyyy-MM-dd')) in ('1', '6', '13')
group by substring(n.ds, 1, 6), datediff(from_unixtime(unix_timestamp(a.ds, 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(unix_timestamp(n.ds, 'yyyyMMdd'), 'yyyy-MM-dd'));
" > cn_fff_retention_monthly.tsv


--- 自由之战付费玩家
insert overwrite table kp_gaea_audit.cn_fff_users_payment_1
select 'gaea', b1.user_id, 'CNY', sum(b1.pay_amount * b2.rate)
from
(
    select user_id, pay_currency, sum(pay_amount) as pay_amount
    from db_billing.gboss_pay_orders
    where ds = '20160714' and region = 'cn' and product_id in ('510002', '530002', '511002', '531002', '520002') and pay_state = '2'
    and from_unixtime(cast(pay_time as bigint), 'yyyyMMdd') <= '20160630'
    group by 'gaea', user_id, pay_currency
) b1
left outer join
(
    select from_currency, rate
    from kp_gaea_audit.currency_exchange_rate
    group by from_currency, rate
) b2
on (b1.pay_currency = b2.from_currency)
group by b1.user_id
union all
select i3.channel, i3.user_id, 'CNY', sum(i1.rmb) as rmb
from
(
    select serverid, sumid as roleid, split(order_channel, '_')[size(split(order_channel, '_')) - 1] as channel, sum(pay_money_fen)/100.0 as rmb
    from db_game_cn_fff.gaea_cn_fff_data_charge_log3
    where ds <='2016-06-30'
        and split(order_channel, '_')[size(split(order_channel, '_')) - 1] not in ('1', '1009', '1010', '1142')
    group by serverid, sumid, split(order_channel, '_')[size(split(order_channel, '_')) - 1]
) i1
left outer join
(
    select id as roleid, accountid, serverid
    from db_game_cn_fff.gaea_cn_fff_data_summoner
    group by id, accountid, serverid
) i2
on(i1.serverid = i2.serverid and i1.roleid = i2.roleid)
left outer join
(
    select id as accountid, split(username, '_')[0] as user_id, split(username, '_')[1] as channel
    from db_game_cn_fff.gaea_cn_fff_role_id_login
    where ds = '2016-07-31'
        and username is not null
        and split(username, '_')[1] not in ('1', '1009', '1010', '1142')
    group by id, username
) i3
on (i2.accountid = i3.accountid)
group by i3.channel, i3.user_id, 'CNY';


select id, count(*) as times, count(distinct username) from db_game_cn_fff.gaea_cn_fff_role_id_login group by 
id having times > 1;





---- 按月统计充值:月份 充值金额  充值人数
select substring(payments.dt, 1, 7), count(distinct payments.channel, payments.user_id), sum(payments.rmb)
from
(
    select 'gaea' as channel, b1.user_id, 'CNY' as pay_currency, b1.dt, sum(b1.pay_amount * b2.rate) as rmb
    from
    (
        select user_id, pay_currency, from_unixtime(cast(pay_time as bigint), 'yyyy-MM-dd') as dt, sum(pay_amount) as pay_amount
        from db_billing.gboss_pay_orders
        where ds = '20160714' 
            and region = 'cn' 
            and product_id in ('510002', '530002', '511002', '531002', '520002') 
            and pay_state = '2'
            and from_unixtime(cast(pay_time as bigint), 'yyyyMMdd') <= '20160630'
        group by user_id, pay_currency, from_unixtime(cast(pay_time as bigint), 'yyyy-MM-dd')
    ) b1
    left outer join
    (
        select from_currency, rate
        from kp_gaea_audit.currency_exchange_rate
        group by from_currency, rate
    ) b2
    on (b1.pay_currency = b2.from_currency)
    group by b1.user_id, b1.dt
    union all
    select i3.channel, i3.user_id, 'CNY' as pay_currency, i1.dt, i1.rmb
    from
    (
        select serverid, sumid, split(order_channel, '_')[size(split(order_channel, '_')) - 1] as channel, substring(date_time, 1, 10) as dt, sum(pay_money_fen)/100.0 as rmb
        from db_game_cn_fff.gaea_cn_fff_data_charge_log3
        where ds <='2016-06-30'
            and split(order_channel, '_')[size(split(order_channel, '_')) - 1] not in ('1', '1009', '1010', '1142')
        group by serverid, sumid, split(order_channel, '_')[size(split(order_channel, '_')) - 1], substring(date_time, 1, 10)
    ) i1
    left outer join
    (
        select id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) i2
    on(i1.serverid = i2.serverid and i1.sumid = i2.id)
    left outer join
    (
        select id as accountid, split(username, '_')[0] as user_id, split(username, '_')[1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where ds = '2016-07-31'
            and username is not null
            and split(username, '_')[1] != '1'
        group by id, username
    ) i3
    on(i2.accountid = i3.accountid)
) payments
group by substring(payments.dt, 1, 7);


---- 重要玩家付费注册时间
select big.user_id, big.channel, big.pay_amount, register.registdate
from
(
    select user_id, channel, pay_amount
    from kp_gaea_audit.cn_fff_users_payment_1
    where cast(pay_amount as bigint) > 1000
) big
left outer join
(
    select t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel) as channel, min(t2.ds) as registdate
    from
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t1
    join
    (
        select server_id, role_id, role_channel, min(ds) ds
        from db_stat_platform.gaea_stat_role_login
        where ds <='20160630' and role_channel != '1'
        group by server_id, role_id, role_channel
    ) t2
    on (t1.role_id = t2.role_id and t1.serverid = t2.server_id)
    left outer join
    (
        select id as accountid, split(username, '_')[0] as user_id, split(username, '_')[1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where ds = '2016-07-31'
            and username is not null
            and split(username, '_')[1] != '1'
        group by id, username
    ) t3
    on(t1.accountid = t3.accountid)
    group by t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel)
) register
on (big.user_id = register.user_id and big.channel = register.channel);


---- 重要玩家登陆天数
select big.user_id, big.channel, big.pay_amount, count(distinct days.ds)
from
(
    select user_id, channel, pay_amount
    from kp_gaea_audit.cn_fff_users_payment_1
    where cast(pay_amount as bigint) > 1000
) big
left outer join
(
    select t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel) as channel, t2.ds
    from
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t1
    join
    (
        select server_id, role_id, role_channel, ds
        from db_stat_platform.gaea_stat_role_login
        where ds <='20160630' and role_channel != '1'
        group by server_id, role_id, role_channel, ds
    ) t2
    on (t1.role_id = t2.role_id and t1.serverid = t2.server_id)
    left outer join
    (
        select id as accountid, split(username, '_')[0] as user_id, split(username, '_')[1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where ds = '2016-07-31'
            and username is not null
            and split(username, '_')[1] != '1'
    ) t3
    on(t1.accountid = t3.accountid)
    group by t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel), t2.ds
) days
on (big.user_id = days.user_id and big.channel = days.channel)
group by big.user_id, big.channel, big.pay_amount;



---- 重要玩家活跃天数
select big.user_id, big.channel, big.pay_amount, count(distinct days.ds)
from
(
    select user_id, channel, pay_amount
    from kp_gaea_audit.cn_fff_users_payment
    where cast(pay_amount as bigint) > 1000
) big
left outer join
(
    select t1.accountid as user_id, if (t2.role_channel in ('1009', '1010', '1142'), 'gaea', t2.role_channel) as channel, t2.ds
    from
    (
        select id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t1
    join
    (
        select server_id, role_id, role_channel, ds
        from db_stat_platform.gaea_stat_role_login
        where ds <='20160630'
        group by server_id, role_id, role_channel, ds
    ) t2
    on (t1.id = t2.role_id and t1.serverid = t2.server_id)
    group by t1.accountid, if (t2.role_channel in ('1009', '1010', '1142'), 'gaea', t2.role_channel), t2.ds
) days
on (big.user_id = days.user_id and big.channel = days.channel);


