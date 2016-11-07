---- 自由之战官方gaea官方渠道标识： 1009(App Store) 1010(GAEA 越狱) 1142(自营:安卓国际包) 1(测试)
---- GBoss游戏标识： '510002', '530002', '511002', '531002', '520002'

---- 自由之战user每月注册用户数：月份  注册用户数
hive -e "
select month, count(distinct user_id, channel)
from 
(
    select t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel) as channel, min(substring(t1.ds, 1, 6)) as month
    from
    (
        select server_id, role_id, role_channel, min(ds) ds
        from db_stat_platform.gaea_stat_role_login
        where ds <='20160930'
            and role_channel != '1'
        group by server_id, role_id, role_channel
    ) t1
    left outer join
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t2
    on (t1.role_id = t2.role_id and t2.serverid = t1.server_id)
    left outer join
    (
        select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] != '1'
        group by id, username
    ) t3
    on(t2.accountid = t3.accountid)
    group by t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel)
) a
group by month;
" > cn_fff_register_monthly.tsv

---- 自由之战user每日注册用户数：月份  注册用户数
hive -e "
select dt, count(distinct user_id, channel)
from 
(
    select t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel) as channel, min(substring(t1.ds, 1, 8)) as dt
    from
    (
        select server_id, role_id, role_channel, min(ds) ds
        from db_stat_platform.gaea_stat_role_login
        where ds <='20160930'
            and role_channel != '1'
        group by server_id, role_id, role_channel
    ) t1
    left outer join
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t2
    on (t1.role_id = t2.role_id and t2.serverid = t1.server_id)
    left outer join
    (
        select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] != '1'
        group by id, username
    ) t3
    on(t2.accountid = t3.accountid)
    group by t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel)
) a
group by dt
order by dt;
" > cn_fff_register_daily.tsv


-----自由之战每月活跃数：月份  活跃用户数
hive -e "
select t1.month, count(distinct t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel))
from
(
    select server_id, role_id, if (role_channel in ('1009', '1010', '1142'), 'gaea', role_channel) as channel, substring(ds, 1, 6) as month
    from db_stat_platform.gaea_stat_role_login
    where ds <='20160930'
        and role_channel != '1'
    group by server_id, role_id, if (role_channel in ('1009', '1010', '1142'), 'gaea', role_channel), substring(ds, 1, 6)
) t1
left outer join
(
    select id as role_id, accountid, serverid
    from db_game_cn_fff.gaea_cn_fff_data_summoner
    group by id, accountid, serverid
) t2
on (t1.role_id = t2.role_id and t2.serverid = t1.server_id)
left outer join
(
    select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] != '1'
        group by id, username
) t3
on (t2.accountid = t3.accountid)
group by t1.month;
" > cn_fff_active_monthly.tsv


-----自由之战每日活跃数：日期  活跃用户数
hive -e "
select t1.dt, count(distinct t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel))
from
(
    select server_id, role_id, if (role_channel in ('1009', '1010', '1142'), 'gaea', role_channel) as channel, substring(ds, 1, 8) as dt
    from db_stat_platform.gaea_stat_role_login
    where ds <='20160930'
        and role_channel != '1'
    group by server_id, role_id, if (role_channel in ('1009', '1010', '1142'), 'gaea', role_channel), substring(ds, 1, 8)
) t1
left outer join
(
    select id as role_id, accountid, serverid
    from db_game_cn_fff.gaea_cn_fff_data_summoner
    group by id, accountid, serverid
) t2
on (t1.role_id = t2.role_id and t2.serverid = t1.server_id)
left outer join
(
    select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] != '1'
        group by id, username
) t3
on (t2.accountid = t3.accountid)
group by t1.dt
order by t1.dt;
" > cn_fff_active_daily.tsv


-----自由之战钻石消耗(event_type>4000 and event_type<5000 钻石消耗)：月份  钻石消耗类型 钻石数量
hive -e "
select substring(ds, 1, 6), event_type, sum(cast(ingot_num as bigint))
from db_stat_platform.gaea_stat_currency_track
where ds <= '20160930'
    and (cast(event_type as bigint) between 4001 and 5000)
group by substring(ds, 1, 6), event_type;
" > cn_fff_yuanbao_use_monthly.tsv


-----自由之战钻石消耗(event_type>4000 and event_type<5000 钻石消耗)：日期  钻石消耗类型 钻石数量
hive -e "
select substring(ds, 1, 8), event_type, sum(cast(ingot_num as bigint))
from db_stat_platform.gaea_stat_currency_track
where ds <= '20160930'
    and (cast(event_type as bigint) between 4001 and 5000)
group by substring(ds, 1, 8), event_type;
" > cn_fff_yuanbao_use_daily.tsv



-----自由之战钻石获得(event_type>3000 and event_type<4000 钻石消耗)：月份  钻石类型 钻石数量
hive -e "
select substring(ds, 1, 6), event_type, sum(cast(ingot_num as bigint))
from db_stat_platform.gaea_stat_currency_track
where ds <= '20160930'
    and (cast(event_type as bigint) between 3001 and 4000)
group by substring(ds, 1, 6), event_type;
" > cn_fff_yuanbao_get_monthly.tsv


-----自由之战钻石充值获得，不包括赠送：月份  钻石类型 钻石数量
hive -e "
select substring(ds, 1, 6), event_type, sum(cast(ingot_num as bigint))
from db_stat_platform.gaea_stat_currency_track
where ds <= '20160930'
    and cast(event_type as bigint) = 3008
group by substring(ds, 1, 6), event_type;
" > cn_fff_yuanbao_pay_monthly.tsv

-----自由之战钻石充值获得，不包括赠送：日期  钻石类型 钻石数量
hive -e "
select substring(ds, 1, 8), event_type, sum(cast(ingot_num as bigint))
from db_stat_platform.gaea_stat_currency_track
where ds <= '20160930'
    and cast(event_type as bigint) = 3008
group by substring(ds, 1, 8), event_type;
" > cn_fff_yuanbao_pay_daily.tsv


---- 自由之战每月留存： 月份， 留存标识， 留存数
--hive -e "
select substring(n.ds, 1, 6), datediff(from_unixtime(unix_timestamp(a.ds, 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(unix_timestamp(n.ds, 'yyyyMMdd'), 'yyyy-MM-dd')), count(distinct a.user_id, a.channel)
from
(
    select t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel) as channel, min(t2.ds) as ds
    from
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t1
    right outer join
    (
        select server_id, role_id, role_channel, min(ds) ds
        from db_stat_platform.gaea_stat_role_login
        where ds <='20160930'
            and role_channel != '1'
        group by server_id, role_id, role_channel
    ) t2
    on (t1.role_id = t2.role_id and t1.serverid = t2.server_id)
    left outer join
    (
        select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] != '1'
        group by id, username
    ) t3
    on (t1.accountid = t3.accountid)
    group by t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel)
) n
join
(
    select t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel) as channel, t2.ds
    from
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t1
    right outer join
    (
        select server_id, role_id, role_channel, ds
        from db_stat_platform.gaea_stat_role_login
        where ds <='20160930'
            and role_channel != '1'
        group by server_id, role_id, role_channel, ds
    ) t2
    on (t1.role_id = t2.role_id and t1.serverid = t2.server_id)
    left outer join
    (
        select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] != '1'
        group by id, username
    ) t3
    on (t1.accountid = t3.accountid)
    group by t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel), t2.ds
) a
on(n.user_id = a.user_id and n.channel = a.channel)
where datediff(from_unixtime(unix_timestamp(a.ds, 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(unix_timestamp(n.ds, 'yyyyMMdd'), 'yyyy-MM-dd')) in ('1', '6', '13')
group by substring(n.ds, 1, 6), datediff(from_unixtime(unix_timestamp(a.ds, 'yyyyMMdd'), 'yyyy-MM-dd'), from_unixtime(unix_timestamp(n.ds, 'yyyyMMdd'), 'yyyy-MM-dd'));
--" > cn_fff_retention_monthly.tsv



--- 自由之战付费玩家建立表格
insert overwrite table kp_gaea_audit.cn_fff_users_payment
select 'gaea', b1.user_id, 'CNY', sum(b1.pay_amount * b2.rate)
from
(
    select user_id, pay_currency, sum(pay_amount) as pay_amount
    from db_billing.gboss_pay_orders
        and region = 'cn'
        and product_id in ('510002', '530002', '511002', '531002', '520002')
        and pay_state = '2'
    and from_unixtime(cast(pay_time as bigint), 'yyyyMMdd') <= '20160930'
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
    from db_game_cn_fff.gaea_cn_fff_data_charge_log
    where ds <='2016-09-30'
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
    select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
    from db_game_cn_fff.gaea_cn_fff_role_id_login
    where username is not null
        and split(username, '_')[size(split(username, '_')) - 1] not in ('1', '1009', '1010', '1142')
    group by id, username
) i3
on (i2.accountid = i3.accountid)
group by i3.channel, i3.user_id, 'CNY';


select id, count(*) as times, count(distinct username) from db_game_cn_fff.gaea_cn_fff_role_id_login
group by id having times > 1;


---- 按月统计充值:月份 充值金额  充值人数
hive -e "
select substring(payments.dt, 1, 7), count(distinct payments.channel, payments.user_id), sum(payments.rmb)
from
(
    select 'gaea' as channel, b1.user_id, 'CNY' as pay_currency, b1.dt, sum(b1.pay_amount * b2.rate) as rmb
    from
    (
        select user_id, pay_currency, from_unixtime(cast(pay_time as bigint), 'yyyy-MM-dd') as dt, sum(pay_amount) as pay_amount
        from db_billing.gboss_pay_orders
        where region = 'cn' 
            and product_id in ('510002', '530002', '511002', '531002', '520002') 
            and pay_state = '2'
            and from_unixtime(cast(pay_time as bigint), 'yyyyMMdd') <= '20160930'
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
        select serverid, sumid as role_id, split(order_channel, '_')[size(split(order_channel, '_')) - 1] as channel, substring(date_time, 1, 10) as dt, sum(pay_money_fen)/100.0 as rmb
        from db_game_cn_fff.gaea_cn_fff_data_charge_log
        where ds <='2016-09-30'
            and split(order_channel, '_')[size(split(order_channel, '_')) - 1] not in ('1', '1009', '1010', '1142')
        group by serverid, sumid, split(order_channel, '_')[size(split(order_channel, '_')) - 1], substring(date_time, 1, 10)
    ) i1
    left outer join
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) i2
    on(i1.serverid = i2.serverid and i1.role_id = i2.role_id)
    left outer join
    (
        select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] not in ('1', '1009', '1010', '1142')
        group by id, username
    ) i3
    on(i2.accountid = i3.accountid)
) payments
group by substring(payments.dt, 1, 7);
" > cn_fff_payment_monthly.tsv

---- 按日期统计充值:日期 充值金额  充值人数
hive -e "
select substring(payments.dt, 1, 10), count(distinct payments.channel, payments.user_id), sum(payments.rmb)
from
(
    select 'gaea' as channel, b1.user_id, 'CNY' as pay_currency, b1.dt, sum(b1.pay_amount * b2.rate) as rmb
    from
    (
        select user_id, pay_currency, from_unixtime(cast(pay_time as bigint), 'yyyy-MM-dd') as dt, sum(pay_amount) as pay_amount
        from db_billing.gboss_pay_orders
        where region = 'cn' 
            and product_id in ('510002', '530002', '511002', '531002', '520002') 
            and pay_state = '2'
            and from_unixtime(cast(pay_time as bigint), 'yyyyMMdd') <= '20160930'
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
        select serverid, sumid as role_id, split(order_channel, '_')[size(split(order_channel, '_')) - 1] as channel, substring(date_time, 1, 10) as dt, sum(pay_money_fen)/100.0 as rmb
        from db_game_cn_fff.gaea_cn_fff_data_charge_log
        where ds <='2016-09-30'
            and split(order_channel, '_')[size(split(order_channel, '_')) - 1] not in ('1', '1009', '1010', '1142')
        group by serverid, sumid, split(order_channel, '_')[size(split(order_channel, '_')) - 1], substring(date_time, 1, 10)
    ) i1
    left outer join
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) i2
    on(i1.serverid = i2.serverid and i1.role_id = i2.role_id)
    left outer join
    (
        select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] not in ('1', '1009', '1010', '1142')
        group by id, username
    ) i3
    on(i2.accountid = i3.accountid)
) payments
group by substring(payments.dt, 1, 10);
" > cn_fff_payment_daily.tsv


---- 重要玩家付费注册时间
hive -e "
select big.user_id, big.channel, big.pay_amount, min(register.registdate) as registdate
from
(
    select user_id, channel, pay_amount
    from kp_gaea_audit.cn_fff_users_payment
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
        where ds <='20160930'
            and role_channel != '1'
        group by server_id, role_id, role_channel
    ) t2
    on (t1.role_id = t2.role_id and t1.serverid = t2.server_id)
    left outer join
    (
        select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] != '1'
        group by id, username
    ) t3
    on(t1.accountid = t3.accountid)
    group by t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel)
) register
on (big.user_id = register.user_id and big.channel = days.channel)
group by big.user_id, big.channel, big.pay_amount;
" > cn_fff_key_users_register.tsv



---- 重要玩家活跃天数
hive -e "
select big.user_id, big.channel, big.pay_amount, count(distinct days.ds)
from
(
    select user_id, channel, pay_amount
    from kp_gaea_audit.cn_fff_users_payment
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
        where ds <='20160930'
            and role_channel != '1'
        group by server_id, role_id, role_channel, ds
    ) t2
    on (t1.role_id = t2.role_id and t1.serverid = t2.server_id)
    left outer join
    (
        select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] != '1'
        group by id, username
    ) t3
    on(t1.accountid = t3.accountid)
    group by t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel), t2.ds
) days
on (big.user_id = days.user_id and big.channel = days.channel)
group by big.user_id, big.channel, big.pay_amount;
" > cn_fff_key_users_days.tsv



---- 重要玩家活跃钻石消耗 按月汇总
hive -e "
select vcurrency.month, vcurrency.event_type, sum(vcurrency.yuanbao)
from
(
    select user_id, channel, pay_amount
    from kp_gaea_audit.cn_fff_users_payment
    where cast(pay_amount as bigint) > 1000
) big
join
(
    select t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel) as channel, t1.month, t1.event_type, sum(t1.yuanbao) as yuanbao
    from
    (
        select substring(ds, 1, 6) month, server_id, role_id, event_type, sum(cast(ingot_num as bigint)) as yuanbao
        from db_stat_platform.gaea_stat_currency_track
        where ds <= '20160930'
            and server_id != '1'
            and (cast(event_type as bigint) between 4001 and 5000)
        group by substring(ds, 1, 6), server_id, role_id, event_type
    ) t1
    left outer join
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t2
    on (t1.role_id = t2.role_id and t1.server_id = t2.serverid)
    left outer join
    (
        select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] != '1'
        group by id, username
    ) t3
    on(t2.accountid = t3.accountid)
    group by t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel), t1.month, t1.event_type
) vcurrency
on (big.user_id = vcurrency.user_id and big.channel = vcurrency.channel)
group by vcurrency.month, vcurrency.event_type
" > cn_fff_key_users_yuanbao_use_monthly.tsv



---- 重要玩家活跃钻石消耗 按月玩家汇总
hive -e "
select big.user_id, big.channel, big.pay_amount, vcurrency.event_type, sum(vcurrency.yuanbao)
from
(
    select user_id, channel, pay_amount
    from kp_gaea_audit.cn_fff_users_payment
    where cast(pay_amount as bigint) > 1000
) big
left outer join
(
    select t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel) as channel, t1.event_type, sum(t1.yuanbao) as yuanbao
    from
    (
        select server_id, role_id, event_type, sum(cast(ingot_num as bigint)) as yuanbao
        from db_stat_platform.gaea_stat_currency_track
        where ds <= '20160930'
            and server_id != '1'
            and (cast(event_type as bigint) between 4001 and 5000)
        group by server_id, role_id, event_type
    ) t1
    left outer join
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t2
    on (t1.role_id = t2.role_id and t1.server_id = t2.serverid)
    left outer join
    (
        select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] != '1'
        group by id, username
    ) t3
    on(t2.accountid = t3.accountid)
    group by t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel), t1.event_type
) vcurrency
on (big.user_id = vcurrency.user_id and big.channel = vcurrency.channel)
group by big.user_id, big.channel, big.pay_amount, vcurrency.event_type
" > cn_fff_key_users_yuanbao_use_detail.tsv


---- 重要玩家活跃钻石充值获得 按月汇总
hive -e "
select vcurrency.month, vcurrency.event_type, sum(vcurrency.yuanbao)
from
(
    select user_id, channel, pay_amount
    from kp_gaea_audit.cn_fff_users_payment
    where cast(pay_amount as bigint) > 1000
) big
join
(
    select t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel) as channel, t1.month, t1.event_type, sum(t1.yuanbao) as yuanbao
    from
    (
        select substring(ds, 1, 6) month, server_id, role_id, event_type, sum(cast(ingot_num as bigint)) as yuanbao
        from db_stat_platform.gaea_stat_currency_track
        where ds <= '20160930'
            and server_id != '1'
            and cast(event_type as bigint) = 3008
        group by substring(ds, 1, 6), server_id, role_id, event_type
    ) t1
    left outer join
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t2
    on (t1.role_id = t2.role_id and t1.server_id = t2.serverid)
    left outer join
    (
        select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] != '1'
        group by id, username
    ) t3
    on(t2.accountid = t3.accountid)
    group by t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel), t1.month, t1.event_type
) vcurrency
on (big.user_id = vcurrency.user_id and big.channel = vcurrency.channel)
group by vcurrency.month, vcurrency.event_type
" > cn_fff_key_users_yuanbao_pay_monthly.tsv



---- 重要玩家活跃钻石充值获得 按月玩家汇总
hive -e "
select big.user_id, big.channel, big.pay_amount, vcurrency.event_type, sum(vcurrency.yuanbao)
from
(
    select user_id, channel, pay_amount
    from kp_gaea_audit.cn_fff_users_payment
    where cast(pay_amount as bigint) > 1000
) big
left outer join
(
    select t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel) as channel, t1.event_type, sum(t1.yuanbao) as yuanbao
    from
    (
        select server_id, role_id, event_type, sum(cast(ingot_num as bigint)) as yuanbao
        from db_stat_platform.gaea_stat_currency_track
        where ds <= '20160930'
            and server_id != '1'
            and cast(event_type as bigint) = 3008
        group by server_id, role_id, event_type
    ) t1
    left outer join
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t2
    on (t1.role_id = t2.role_id and t1.server_id = t2.serverid)
    left outer join
    (
        select id as accountid, substring(username, 1, length(username) - length(split(username, '_')[size(split(username, '_')) - 1]) - 1) as user_id, split(username, '_')[size(split(username, '_')) - 1] as channel
        from db_game_cn_fff.gaea_cn_fff_role_id_login
        where username is not null
            and split(username, '_')[size(split(username, '_')) - 1] != '1'
        group by id, username
    ) t3
    on(t2.accountid = t3.accountid)
    group by t3.user_id, if (t3.channel in ('1009', '1010', '1142'), 'gaea', t3.channel), t1.event_type
) vcurrency
on (big.user_id = vcurrency.user_id and big.channel = vcurrency.channel)
group by big.user_id, big.channel, big.pay_amount, vcurrency.event_type
" > cn_fff_key_users_yuanbao_pay_detail.tsv



----- 付费玩家钻石消耗
hive -e "
select substring(track.event_time, 1, 7), track.event_type, sum(track.yuanbao)
from
(
   select t2.accountid, min(t1.paytime) as firstpaytime
    from 
    (
        select serverid, sumid as role_id, min(date_time) as paytime
        from db_game_cn_fff.gaea_cn_fff_data_charge_log
        where ds <='2016-09-30'
            and split(order_channel, '_')[size(split(order_channel, '_')) - 1] != '1'
        group by serverid, sumid
    ) t1
    join
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        group by id, accountid, serverid
    ) t2
    on (t1.serverid = t2.serverid and t1.role_id = t2.role_id)
    group by t2.accountid
) payment
join
(
    select y2.accountid, y1.event_time, y1.event_type, y1.yuanbao
    from
    (    
        select server_id, role_id, event_time, event_type, cast(ingot_num as bigint) as yuanbao
        from db_stat_platform.gaea_stat_currency_track
        where ds <= '20160930'
            and server_id != '1'
    ) y1
    join
    (
        select id as role_id, accountid, serverid
        from db_game_cn_fff.gaea_cn_fff_data_summoner
        where ds = '2016-07-26'
        group by id, accountid, serverid
    ) y2
    on (y1.server_id = y2.serverid and y1.role_id = y2.role_id)
) track
on (payment.accountid = track.accountid)
where (track.event_time >= payment.firstpaytime)
group by substring(track.event_time, 1, 7), track.event_type;
" > cn_fff_key_users_yuanbao_diamond_use.tsv
