---- 测试服务器：'249','49'

----- 每月注册，通过首次登陆时间计算
select substring(first_login_date, 1, 7) as joinmonth, count(distinct o1.user_id) as count_userid
from 
(
    select user_id, min(substring(created_at, 1, 10)) as first_login_date
    from db_game_g_dhd.gaea_g_dhd_login_log
    where substring(created_at, 1, 7) between '2016-01' and '2016-09'
    group by user_id
) o1
group by substring(first_login_date, 1, 7)
order by joinmonth;


----- 每月注册，通过首次登陆时间计算
select first_login_date, count(distinct o1.user_id) as count_userid
from 
(
    select user_id, min(substring(created_at, 1, 10)) as first_login_date
    from db_game_g_dhd.gaea_g_dhd_login_log
    where substring(created_at, 1, 7) between '2016-01' and '2016-09'
    group by user_id
) o1
group by first_login_date
order by first_login_date;


----- 每月活跃
select substring(created_at, 1, 7) as mon, count(distinct user_id)
from db_game_g_dhd.gaea_g_dhd_login_log
where substring(created_at, 1, 7) between '2016-01' and '2016-09'
group by substring(created_at, 1, 7)
order by mon;


----- 每日活跃
select substring(created_at, 1, 10) as dt, count(distinct user_id)
from db_game_g_dhd.gaea_g_dhd_login_log
where substring(created_at, 1, 7) between '2016-01' and '2016-09'
group by substring(created_at, 1, 10)
order by dt;


---- 每月付费用户数&金额
select substring(created_at, 1, 7) as mon, count(distinct user_id), sum(cast(amount_cents as double))/100.0 as payamount
from db_game_g_dhd.gaea_g_dhd_payments
where ds between '2016-01-01' and '2016-09-30'
group by substring(created_at, 1, 7)
order by mon;


---- 每日付费用户数&金额
select substring(created_at, 1, 10) as dt, count(distinct user_id), sum(cast(amount_cents as double))/100.0 as payamount
from db_game_g_dhd.gaea_g_dhd_payments
where ds between '2016-01-01' and '2016-09-30'
group by substring(created_at, 1, 10)
order by dt;



--Kabam 龙族崛起 DHD 用户付费表，加虚拟币获得数，建表(数据) 新 #33931
insert into table kp_gaea_audit.kabam_dhd_key_user
select user_id as userid, sum(cast(amount_cents as bigint))/100.0 as payamount, sum(cast(`value` as bigint)) as vcurrency
from db_game_g_dhd.gaea_g_dhd_payments
where ds between '2016-01-01' and '2016-09-30'
group by user_id
order by payamount desc;



--11. 重要玩家的钻石余额 
--userid pay_usd vcurrency_get vcurrency_balance 
--#1472
select changestatus.userid, changestatus.payamount, changestatus.vcurrency_get, sum(finalstatus.vcurrency - changestatus.vcurrency) as vcurrency_use
from
(
    select keyuserlast.userid, keyuserlast.payamount, keyuserlast.vcurrency_get, sum(transactionsafter.vcurrency) as vcurrency
    from
    (
        select keyuser.userid, keyuser.payamount, keyuser.vcurrency_get, transactionlastds.lastds
        from
        (
            select userid, payamount, vcurrency as vcurrency_get
            from kp_gaea_audit.kabam_dhd_key_user 
            where cast(payamount as double) > 1000
        ) keyuser
        left outer join
        (
            select user_id as userid, max(unix_timestamp(created_at,'yyyy-MM-dd HH:mm:ss')) as lastds   
            from db_game_g_dhd.gaea_g_dhd_user_rubies
            where ds between '2016-01-01' and '2016-09-30'
                and rubies != 0
                and realm_id != '249' and realm_id != '49'
                and ruby_type = '0'
            group by user_id                   
        ) transactionlastds
        on(keyuser.userid = transactionlastds.userid)
    ) keyuserlast
    left outer join
    (    
        select trdt.user_id as userid, trdt.rubies as vcurrency, trdt.created_at as createddate
        from 
        (
            select user_id, rubies, created_at
            from db_game_g_dhd.gaea_g_dhd_user_rubies
            where (ds between '2016-01-01' and '2016-12-31')
                and realm_id != '249' and realm_id != '49'
                and rubies != 0
                and ruby_type = '0'
        ) trdt
        join
        (
            select max(unix_timestamp(updated_at,'yyyy-MM-dd HH:mm:ss')) as mdt
            from db_game_g_dhd.gaea_g_dhd_users
            where ds = '2016-10-11'
        ) accmdt
        on (true)
        where unix_timestamp(created_at,'yyyy-MM-dd HH:mm:ss') - accmdt.mdt <= 0        
    ) transactionsafter
    on (keyuserlast.userid = transactionsafter.userid)
    where unix_timestamp(transactionsafter.createddate,'yyyy-MM-dd HH:mm:ss') - keyuserlast.lastds >= 0
    group by keyuserlast.userid, keyuserlast.payamount, keyuserlast.vcurrency_get
) changestatus
left outer join
(
    select id as userid, rubies as vcurrency
    from db_game_g_dhd.gaea_g_dhd_users
    where ds = '2016-10-11'
) finalstatus
on (changestatus.userid = finalstatus.userid)
group by changestatus.userid, changestatus.payamount, changestatus.vcurrency_get;




----------------------------------------------------------------
--Kabam 龙族崛起 DHD(/DOAH) 分平台计 充值金额(USD)
----------------------------------------------------------------
select platform, sum(cast(amount_cents as bigint))/100.0 
from db_game_g_dhd.gaea_g_dhd_payments
where ds between '2016-01-01' and '2016-09-30'
    and realm_id not in ('249','49')
group by platform;
--对比数据
GooglePlay	5794647.49
TrialPay	10034.48
iTunes	4215406.1
loyalty	0.0


----------------------------------------------------------------
--Kabam 龙族崛起 DHD(/DOAH) 分月计 重要玩家充值金额(USD)
----------------------------------------------------------------
select substring(o2.created_at,1,7) as paymonth, sum(o2.amount_cents)/100.0 as pay_usd
from
(
    select userid as user_id
    from kp_gaea_audit.kabam_dhd_key_user 
    where cast(payamount as double) > 1500
) o1
join
(
    select user_id, created_at, cast(amount_cents as bigint) as amount_cents
    from db_game_g_dhd.gaea_g_dhd_payments
    where ds between '2016-01-01' and '2016-09-30'
        and realm_id not in ('249', '49')
) o2
on (o1.user_id = o2.user_id)
group by substring(o2.created_at,1,7)
order by paymonth;

----------------------------------------------------------------
--Kabam 龙族崛起 DHD(/DOAH) 分消耗类型计 消耗虚拟币数量(vcurrency)
----------------------------------------------------------------
select tag, sum(-cast(rubies as bigint)) as vcurrency
from db_stat_kabam_mysql.doah_user_rubies
where ds between '2016-01-01' and '2016-09-30'    
    and cast(rubies as bigint) < 0
    and realm_id not in ('249', '49')
    and ruby_type = '0'
group by tag
order by tag;









---=======================================deprecated==================
--Kabam 龙族崛起 DHD(/DOAH) 分月计 加入玩家数 (按users表)  没有用
select substring(created_at,1,7) as joinmonth, count(distinct id)
from db_game_g_dhd.gaea_g_dhd_users
where ds = '2016-10-11'
    and substring(created_at,1,7) between '2016-01' and '2016-09'
group by substring(created_at,1,7)
order by joinmonth;




----------------------------------------------------------------
--充值? 分渠道 (对比数据用)
----------------------------------------------------------------
select platform, count(distinct id), sum(cast(`value` as bigint)) 
from db_game_g_doah.doah_payments 
where ds between '2016-01-01' and '2016-09-30'
    and realm_id not in ('249', '49')
group by platform;
--对比数据: 渠道 次数 获得虚拟币?
GooglePlay  147418  58133800
TrialPay    4170    70123
iTunes  77483   41104310
loyalty 24020   786935
