-- 累计注册用户（个）
-- gaea官方的没有渠道前缀，其他都有

CREATE EXTERNAL TABLE `kp_gaea_audit.cn_xjqxz_payers_201609`(
    `user_id` string,
    `cny` double
)ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS PARQUET;

insert overwrite table kp_gaea_audit.cn_xjqxz_payers_201609
select payment.user_id, sum(payment.amount * rates.rate) as amount
from
(
  select userid as user_id, currency,
  CASE channel
    WHEN 'tencent_ysdk' THEN amount / 10
    ELSE amount
    END
  as amount
  from db_billing.hj_order
  where region = 'cn'
    and game_name = 'xianjian'
    and channel != 'test'
    and status in ('1','2','4')
    and substring(createtime, 1, 10) between '2016-07-21' and '2016-09-30'
  union all
  select user_id, pay_currency as currency, pay_amount as amount
  from db_billing.gboss_pay_orders
  where from_unixtime(cast(substring(pay_time,1,10) as int),'yyyyMMdd') between '20160811'  and '20160930'
    and product_id = '510017'
    and channel_id = '14'
    and pay_state = '2'
) payment
left outer join
(
  select from_currency, rate
  from kp_gaea_audit.currency_exchange_rate
) rates
on(payment.currency = rates.from_currency)
group by payment.user_id;


---- 重要玩家充值获得虚拟货币(所有/充值获得)
select ku.user_id, ku.cny, yb.coinnum
from
(
  select user_id, cny
  from kp_gaea_audit.cn_xjqxz_payers_201609
  where cast(cny as double) > 3000
) ku
left outer join
(
  select accountid as user_id, cointype, reason, sum(coinnum) as coinnum
  from xjqxz.gaea_s_cm_vcurrency
  where ds between '20160721' and '20160930'
    and reason = '充值获得的元宝(绑定或通用)'
    and cointype = 'yuanBao'
  group by accountid, cointype, reason
) yb
on (ku.user_id = yb.user_id);


---- User-id、每个user-id充值金额、每个user-id注册日期(done)
select ku.user_id, ku.cny, register.registerdate
from
(
  select user_id, cny
  from kp_gaea_audit.cn_xjqxz_payers_201609
  where cast(cny as double) > 3000
) ku
left outer join
(
  select min(from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd')) as registerdate, gaeaid as user_id
  from db_stat_platform.gaea_stat_userlogin
  where ds between '20160721' and '20161030'
    and appid = 'cn.xjqxz'
  group by gaeaid
) register
on (ku.user_id = register.user_id)
order by register.registerdate asc;


---- 重要玩家充值获得虚拟货币(充值获得)
select ku.user_id, ku.cny, yb.coinnum
from
(
  select user_id, cny
  from kp_gaea_audit.cn_xjqxz_payers_201609
  where cast(cny as double) > 3000
) ku
left outer join
(
  select accountid as user_id, sum(coinnum) as coinnum
  from xjqxz.gaea_s_cm_vcurrency
  where ds between '20160721' and '20160930'
    and reason = '充值获得的元宝(绑定或通用)'
    and cointype = 'yuanBao'
    and addtype = 0
  group by accountid
) yb
on (ku.user_id = yb.user_id);


---- 重要玩家消耗虚拟货币(所有)
select ku.user_id, yb.reason, sum(yb.coinnum)
from
(
  select user_id, cny
  from kp_gaea_audit.cn_xjqxz_payers_201609
  where cast(cny as double) > 3000
) ku
left outer join
(
  select accountid as user_id, sum(coinnum) as coinnum, reason
  from xjqxz.gaea_s_cm_vcurrency
  where ds between '20160721' and '20160930'
    and cointype in ('yuanBao','bindingYuanbao')
    and addtype = 1
  group by accountid, reason
) yb
on (ku.user_id = yb.user_id)
group by ku.user_id, reason;


-- 重要玩家活跃天数
select ku.user_id, count(distinct login.ds) as active_days
from
(
  select user_id
  from kp_gaea_audit.cn_xjqxz_payers_201609
  where cast(cny as double) > 3000
) ku
left outer join
(
  select ds, gaeaid as user_id
  from db_stat_platform.gaea_stat_userlogin
  where ds between '20160721' and '20160930'
    and appid = 'cn.xjqxz'
  group by ds, gaeaid
) login
on (ku.user_id = login.user_id)
group by ku.user_id;


-- 每月累计注册人数
select substring(register.registerdate, 1, 6) as month, count(distinct ku.user_id) 
from
(
  select user_id
  from kp_gaea_audit.cn_xjqxz_payers_201609
  where cast(cny as double) > 3000
) ku
left outer join
(
  select min(ds) as registerdate, gaeaid as user_id
  from db_stat_platform.gaea_stat_userlogin
  where ds between '20160721' and '20160930'
    and appid = 'cn.xjqxz'
  group by gaeaid
) register
on (ku.user_id = register.user_id)
group by substring(register.registerdate, 1, 6)
order by month asc;


-- 每月累计活跃人数
select login.month, count(distinct ku.user_id) as active_user
from
(
  select user_id
  from kp_gaea_audit.cn_xjqxz_payers_201609
  where cast(cny as double) > 3000
) ku
left outer join
(
  select substring(ds,1,6) as month, gaeaid as user_id
  from db_stat_platform.gaea_stat_userlogin
  where ds between '20160721' and '20160930'
    and appid = 'cn.xjqxz'
  group by substring(ds,1,6), gaeaid
) login
on (ku.user_id = login.user_id)
group by login.month;


---- 每个user-id角色等级(done)
select ku.user_id, level.level from
(
  select user_id
  from kp_gaea_audit.cn_xjqxz_payers_201609
  where cast(cny as double) > 3000
) ku
left outer join
(
  select accountid as user_id, max(level) as level
  from xjqxz.gaea_s_rolelevel
  where ds between '20160721' and '20160930'
  group by accountid
) level
on(ku.user_id = level.user_id);


---- 每个user-id邀请好友数量
select ku.user_id, count(distinct invite.invitedid) as invited_user
from
(
  select user_id
  from kp_gaea_audit.cn_xjqxz_payers_201609
  where cast(cny as double) > 3000
) ku
left outer join
(
  select accountid as user_id, invitedid
  from xjqxz.gaea_s_cm_invitefriend
  where ds between  '20160721' and '20160930'
  group by accountid, invitedid
) invite
on(ku.user_id = invite.user_id)  
group by ku.user_id;
