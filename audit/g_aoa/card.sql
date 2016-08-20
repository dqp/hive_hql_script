---- AOA 未通过钻石直接购买道具语句
select t1.user_id, from_unixtime(cast(t1.pay_time as bigint), 'yyyy-MM-dd HH:mm:ss'), t1.pay_ip, t1.pay_lcoins, t1.pay_currency, t1.pay_amount, t2.item, t2.itemname, t1.product_id
from
(
    select user_id, pay_ip, product_id, order_id, pay_time, pay_lcoins, pay_currency, pay_amount
    from db_billing.gboss_pay_orders
    where ds = '20160714' and region = 'en' and product_id in ('720002', '721002', '723002', '710002') and pay_state = '2' and pay_amount > 0 and pay_lcoins > 0
    and from_unixtime(cast(pay_time as bigint), 'yyyy-MM-dd') <= '2016-06-30'
) t1
join
(
    select product_id, exchange_id, item, itemname
    from db_billing.gboss_exchange_orders
    where ds = '20160714' and region = 'en' and product_id in ('720002', '721002', '723002', '710002') 
    and item in ('com.gaeamobile.en.aoa.w2t10g0','com.gaeamobile.en.aoa.w1t5g0','com.gaeamobile.en.aoa.w1t2g0','com.gaeagame.en.aoa.w2g280','com.gaeagame.en.aoa.w1g2','com.gaeagame.en.aoa.w1g140','1000018082','1000018080','0910034802','0910034800')
    group by product_id, exchange_id, item, itemname
) t2
on(t1.product_id = t2.product_id and t1.order_id = t2.exchange_id);