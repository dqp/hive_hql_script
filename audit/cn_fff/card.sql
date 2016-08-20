---- 自由之战 未通过钻石直接购买道具语句
select t1.user_id, from_unixtime(cast(t1.pay_time as bigint), 'yyyy-MM-dd HH:mm:ss'), t1.pay_ip, t1.pay_lcoins, t1.pay_currency, t1.pay_amount, t2.item, t2.itemname, t1.product_id
from
(
    select user_id, pay_ip, product_id, order_id, pay_time, pay_lcoins, pay_currency, pay_amount
    from db_billing.gboss_pay_orders
    where ds = '20160714' and region = 'cn' and product_id in ('510002', '530002', '511002', '531002', '520002') and pay_state = '2' and pay_amount > 0 and pay_lcoins > 0
    and from_unixtime(cast(pay_time as bigint), 'yyyy-MM-dd') <= '2016-06-30'
) t1
join
(
    select product_id, exchange_id, item, itemname
    from db_billing.gboss_exchange_orders
    where ds = '20160714' and region = 'cn' and product_id in ('510002', '530002', '511002', '531002', '520002')
    and item in ('com.gaeagame.cn.fff.w1t4g1','com.gaeagame.cn.fff.w1t2g1','com.gaeagame.cn.fff.m1t15g1','com.gaeagame.cn.fffpad.w1t4g1','com.gaeagame.cn.fffpad.w1t2g1','com.gaeagame.cn.fffpad.w2t8g1','com.gaeagame.cn.fffpadyy.w1t4g1','com.gaeagame.cn.fffpadyy.w1t2g1','com.gaeagame.cn.fffpadyy.w2t8g1','com.gaeagame.cn.fffyy.w1t4g1','com.gaeagame.cn.fffyy.w1t2g1','com.gaeagame.cn.fffyy.m1t15g1')
    group by product_id, exchange_id, item, itemname
) t2
on(t1.product_id = t2.product_id and t1.order_id = t2.exchange_id);
