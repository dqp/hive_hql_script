-- 累计注册用户（个）
select count(distinct gaeaid) from
(
  select min(ds) as ds, gaeaid
  from db_stat_platform.gaea_stat_userlogin
  where ds between '20160721' and '20160930'
    and appid = 'cn.xjqxz'
  group by gaeaid
) firstLogin


-- 每天新增注册玩家（个）
select ds, count(distinct gaeaid) from
(
  select min(ds) as ds, gaeaid
  from db_stat_platform.gaea_stat_userlogin
  where ds between '20160721' and '20160930'
    and appid = 'cn.xjqxz'
  group by gaeaid
) firstLogin
group by ds
order by ds asc;


-- 每天活跃用户数（个）
select ds, count(distinct gaeaid)
from db_stat_platform.gaea_stat_userlogin
where ds between '20160721' and '20160930'
  and appid = 'cn.xjqxz'
group by ds
order by ds asc;


-- 每月活跃用户数（个）
select substring(ds, 1, 6) as month, count(distinct gaeaid)
from db_stat_platform.gaea_stat_userlogin
where ds between '20160721' and '20160930'
  and appid = 'cn.xjqxz'
group by ds
order by month asc


-- 每天付费用户数（个）\ 充值流水（元）
-- 每天付费用户数（个）\ 充值流水（元）
select  from_unixtime(cast(substring(createtime,1,10) as int),'yyyy-MM-dd') as ds , count(distinct userid), sum(amount)
from
(
  select createtime, channel, userid, currency, amount*rate.rate as amount
  from
  (
    select UNIX_TIMESTAMP(createtime) as createtime, channel, userid, currency,
        CASE channel
            WHEN 'tencent_ysdk' THEN amount / 10
            ELSE amount
            END
        as  amount
        from db_billing.hj_order
        where region = 'cn'
          and game_name = 'xianjian'
          and status in ('1','2','4')
          and substring(createtime, 1, 10) between '2016-07-21' and '2016-09-30'
    union all      
    select cast(substring(pay_time,1,10) as int) as createtime, 'appstore', user_id as userid, pay_currency as currency, pay_amount as amount
    from db_billing.gboss_pay_orders
    where from_unixtime(cast(substring(pay_time,1,10) as int),'yyyyMMdd') between '20160811'  and '20160930'
        and product_id = '510017'
        and channel_id = '14'
        and pay_state = '2'
  ) pay
  join
  (
    select from_currency, rate
    from kp_gaea_audit.currency_exchange_rate
  ) rate
  on(pay.currency = rate.from_currency)
  where channel != 'test'
) payment
group by  from_unixtime(cast(substring(createtime,1,10) as int),'yyyy-MM-dd')
order by ds asc


-- 每月付费用户数（个）、充值流水（元）(分渠道)
-- 每月付费用户数（个）、充值流水（元）
select  from_unixtime(cast(substring(createtime,1,10) as int),'yyyy-MM') as month , channel, count(distinct userid), sum(amount)
from
(
  select createtime, channel, userid, currency, amount*rate.rate as amount
  from
  (
    select UNIX_TIMESTAMP(createtime) as createtime, channel, userid, currency,
        CASE channel
            WHEN 'tencent_ysdk' THEN amount / 10
            ELSE amount
            END
        as  amount
        from db_billing.hj_order
        where region = 'cn'
          and game_name = 'xianjian'
          and status in ('1','2','4')
          and substring(createtime, 1, 10) between '2016-07-21' and '2016-09-30'
    union all      
    select cast(substring(pay_time,1,10) as int) as createtime, 'appstore', user_id as userid, pay_currency as currency, pay_amount as amount
    from db_billing.gboss_pay_orders
    where from_unixtime(cast(substring(pay_time,1,10) as int),'yyyyMMdd') between '20160811'  and '20160930'
        and product_id = '510017'
        and channel_id = '14'
        and pay_state = '2'
  ) pay
  join
  (
    select from_currency, rate
    from kp_gaea_audit.currency_exchange_rate
  ) rate
  on(pay.currency = rate.from_currency)
  where channel != 'test'
) payment
group by  from_unixtime(cast(substring(createtime,1,10) as int),'yyyy-MM'), channel, currency
order by month, channel, currency asc


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
where datediff(login.logindate, register.registerdate)  in (1, 6, 13)
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
