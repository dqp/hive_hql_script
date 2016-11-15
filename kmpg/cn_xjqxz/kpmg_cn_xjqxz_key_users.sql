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


---- 重要玩家充值获得虚拟货币
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


-- 每天充值虚拟货币数
select ds, cointype, reason, sum(coinnum) as coinnum
from xjqxz.gaea_s_cm_vcurrency
where ds between '20160721' and '20160930'
  and reason = '充值获得的元宝(绑定或通用)'
  and cointype = 'yuanBao'
group by ds, cointype, reason
order by ds asc


-- 每天消耗虚拟货币数
select ds, sum(coinnum)
from xjqxz.gaea_s_cm_vcurrency
where ds between '20160721' and '20160930'
  and cointype in ('yuanBao', 'bindingYuanbao')
  and addtype = 1
group by ds  


-- 每日经好友推荐后注册用户数
--  invitedid 被邀请的玩家的角色id，通过roleid 和 serverid 唯一确定一个gaeaid
select register.ds, count(distinct gaeaid) from
(
  select min(ds), invitedid, gameregion
  from xjqxz.gaea_s_cm_invitefriend
  where ds between  '20160721' and '20160930'
  group by invitedid, gameregion
) invitefriend
join
(
  select ds, accountid, gaeaid, serverid
  from db_stat_platform.gaea_stat_login
  where ds between '20160721' and '20160930'
    and appid = 'cn.xjqxz'
    and action = '0'
) register
on (invitefriend.gameregion = register.serverid and invitefriend.invitedid = register.accountid)
group by register.ds


-- 经好友推荐后注册用户的次日留存率,7日留存率,14日留存率
hive -S -e "
select register.ds, datediff(login.logindate, register.registerdate) as retentiondays, count(distinct login.gaeaid) from
(
  select min(ds), invitedid, gameregion
  from xjqxz.gaea_s_cm_invitefriend
  where ds between  '20160818' and '20161106'
  group by invitedid, gameregion
) invitefriend
join
(
  select ds, from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') as registerdate,accountid, gaeaid, serverid
  from db_stat_platform.gaea_stat_login
  where ds between '20160818' and '20161106'
    and appid = 'cn.xjqxz'
    and action = '0'
) register
on (invitefriend.gameregion = register.serverid and invitefriend.invitedid = register.accountid)
join
(
  select ds, from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') as logindate,accountid, gaeaid, serverid
  from db_stat_platform.gaea_stat_login
  where ds between '20160818' and '20161106'
    and appid = 'cn.xjqxz'
    and accountid is not null
    and serverid is not null
) login
on(register.gaeaid = login.gaeaid)
where datediff(login.logindate, register.registerdate)  in (1, 7, 14)
group by register.ds, datediff(login.logindate, register.registerdate)
order by register.ds, retentiondays asc
" > invitedFriendRetention0818_1108.txt


-- 游戏内交易行的日交易类型、交易数
-- warestype 商品类型
hive -S -e "
select ds, warestype, count(*)
from xjqxz.gaea_s_cm_auditing
where ds between '20160721' and '20160930'
group by ds, warestype
order by ds , warestype asc
" > auditingTypeNum0818_1108.txt


-- 所有玩家次日留存率、7日留存率、14日留存率、30日留存率。
hive -S -e "
select register.registerdate, datediff(login.logindate, register.registerdate) as retentiondays, count(distinct login.gaeaid) from
(
  select from_unixtime(unix_timestamp(min(ds), 'yyyyMMdd'), 'yyyy-MM-dd') as registerdate, gaeaid
  from db_stat_platform.gaea_stat_userlogin
  where ds between '20160721' and '20160930'
    and appid = 'cn.xjqxz'
  group by gaeaid
) register
join
(
  select from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') as logindate, gaeaid
  from db_stat_platform.gaea_stat_userlogin
  where ds between '20160721' and '20160930'
    and appid = 'cn.xjqxz'
  group by from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd'), gaeaid
) login
on(register.gaeaid = login.gaeaid)
where datediff(login.logindate, register.registerdate) in (1, 7, 14)
group by register.registerdate, datediff(login.logindate, register.registerdate)
order by register.registerdate, retentiondays asc
" > allUserRetention0818_1106.txt
