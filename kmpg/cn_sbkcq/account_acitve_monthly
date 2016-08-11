select substring(ds, 1, 6) as mon, count(distinct server_id, account_id)
from db_stat_sbkcq.gaea_cn_sbkcq_login_log
where ds <= '20160630'
group by substring(ds, 1, 6)
order by mon
