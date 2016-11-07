-- 每月付费金额
select substring(ds,1,7) as paymonth, 
    sum(cast(cast(currency as double)*1000.0 as bigint))/1000.0 as pay_usd,
    sum(cast(diamonds as bigint)) as vcurrency_get
from db_game_web_wt.gaea_web_wt_charge
group by substring(ds,1,7)
order by paymonth;