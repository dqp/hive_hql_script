
select cps.tag, bill.month, sum(bill.rmb)
from
(
    select tag 
    from db_tmp.cn_sbkcq_cps
) cps
join
(
    select tag1, serverid, accountid
    from db_tmp.gaea_stat_platform
    where ds between '20160128' and '20160731'
        and appid = 'cn.sbkcq'
        and action in ('0', '1')
        and tag1 is not null
        and tag1 != ''
    group by tag1, serverid, accountid
) sdk
on (cps.tag = sdk.tag1)
join
(
    select substring(ds, 1, 6) as month, server_id, account_id, sum(cast(yuanbao as bigint) / 10) as rmb
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds >= '20160128'
    group by substring(ds, 1, 6), server_id, account_id
) bill
on (sdk.serverid = bill.server_id and sdk.accountid = bill.account_id)
group by cps.tag, bill.month;
