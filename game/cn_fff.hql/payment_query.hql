set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.max.dynamic.partitions.pernode=100000;
set hive.exec.max.dynamic.partitions=100000;

insert overwrite table db_game_cn_fff.gaea_cn_fff_data_charge_log 
    partition(ds, serverid)
select order_channel,orderid_channel,channelaccount,date_time,productid,last_login_time,
    result,pay_money_fen,normal_addcredit,add_credit,keep_time_sec,sumid,
    substring(date_time,1,10),serverid
from db_game_origin_mysql_backup.gaea_cn_fff_data_charge_log
where ds = '2016-07-26';



insert overwrite table kp_gaea_audit.cn_fff_users_payment
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