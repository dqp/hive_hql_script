-- 每月付费金额
select substring(ds,1,7) as paymonth, 
    sum(cast(cast(currency as double)*1000.0 as bigint))/1000.0 as pay_usd,
    sum(cast(diamonds as bigint)) as vcurrency_get
from db_game_web_wt.gaea_web_wt_charge
group by substring(ds,1,7)
order by paymonth;


----------------------------------------------------------------
--每日付费用户数、获得虚拟币数、付费金额
----------------------------------------------------------------
select ds, 
    count(distinct udid) as pay_count, sum(cast(diamonds as bigint)) as vcurrency_get, round(sum(cast(currency as double)),2) as pay_usd
from db_game_web_wt.gaea_web_wt_charge
where ds between '2016-01-01' and '2016-09-30'
group by ds
order by ds;

----------------------------------------------------------------
--每月付费用户数
----------------------------------------------------------------
select substring(ds,1,7) as pay_month, count(distinct udid) 
from db_game_web_wt.gaea_web_wt_charge
where ds between '2016-01-01' and '2016-09-30'
group by substring(ds,1,7)
order by pay_month;

----------------------------------------------------------------
--充值流水按平台、按月汇总；付费金额(USD)
----------------------------------------------------------------
select paychannel, substring(ds,1,7) as pay_month,    
    round(sum(cast(currency as double)),2) as pay_usd
from db_game_web_wt.gaea_web_wt_charge
where ds between '2016-01-01' and '2016-09-30'
group by paychannel, substring(ds,1,7)
order by paychannel, pay_month;