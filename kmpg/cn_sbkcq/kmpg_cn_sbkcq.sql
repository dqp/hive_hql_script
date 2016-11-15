---- 截止2016年9月30日，有过合服的日期有：'20160508','20160510','20160516','20160615','20160712','20160718','20160808','20160816','20160912','20161031'

---- users注册数据
select tmp.registerdate as dt, substring(tmp.registerdate, 1, 6) as mon, count(distinct tmp.third_type, tmp.third_id)
from
(
    select t2.third_type, t2.third_id, min(t1.ds) as registerdate
    from
    (
        select ds, region_id, account_id
        from db_stat_sbkcq.gaea_cn_sbkcq_login_log
        where ds between '20160128' and '20160930'
        group by ds, region_id, account_id
    ) t1
    join
    (
        select region_id, account_id, third_type, third_id
        from db_stat_sbkcq.cn_sbkcq_user_accout_map
        group by region_id, account_id, third_type, third_id
    ) t2
    on (t1.region_id = t2.region_id and t1.account_id = t2.account_id)
    group by t2.third_type, t2.third_id
) tmp
group by tmp.registerdate, substring(tmp.registerdate, 1, 6)
order by dt, mon;


---- user月活跃
select t1.mon, count(distinct t2.third_type, t2.third_id) as users
from
(
    select substring(ds, 1, 6) as mon, region_id, account_id
    from db_stat_sbkcq.gaea_cn_sbkcq_login_log
    where ds between '20160128' and '20160930'
    group by substring(ds, 1, 6), region_id, account_id
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.cn_sbkcq_user_accout_map
    group by region_id, account_id, third_type, third_id
) t2
on (t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by t1.mon
order by t1.mon;


---- user按照日期活跃
select t1.ds, count(distinct t2.third_type, t2.third_id) as users
from
(
    select ds, region_id, account_id
    from db_stat_sbkcq.gaea_cn_sbkcq_login_log
    where ds between '20160128' and '20160930'
    group by ds, region_id, account_id
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.cn_sbkcq_user_accout_map
    group by region_id, account_id, third_type, third_id
) t2
on (t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by t1.ds
order by t1.ds;


---- 每月yuanbao各类型获得
select ds, substring(ds, 1, 6) as mon, cmd_id, sum(cast(num as bigint)) as yuanbaos
from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
where ds between '20160128' and '20160930'
    and container_type = '3'
    and cast(num as bigint) > 0
    and cmd_id not in ('211', '213', '1733')
group by ds, substring(ds, 1, 6), cmd_id
order by ds, mon, cmd_id;


---- 每月yuanbao各类型消耗
select tmp.ds, substring(tmp.ds, 1, 6) as mon, tmp.cmd_id, sum(tmp.yuanbaos) as yuanbaos
from
(
    select ds, cmd_id, sum(cast(num as bigint)) as yuanbaos
    from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
    where ds between '20160128' and '20160930'
        and container_type = '3'
        and cast(num as bigint) < 0
        and cmd_id not in ('211', '213', '1733')
    group by ds, cmd_id
    union all
    select ds, concat(cmd_id, '_', 'tax') as cmd_id, sum(if(cast(num as bigint) * 0.03 > 150, 150, floor(cast(num as bigint) * 0.03))) as yuanbaos
    from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
    where ds between '20160128' and '20160930'
        and container_type = '3'
        and cmd_id in ('213', '1733')
    group by ds, cmd_id
) tmp
group by tmp.ds, substring(tmp.ds, 1, 6), tmp.cmd_id
order by tmp.ds, mon, tmp.cmd_id;

---- 按照日期充值人数、金额
select pay.ds, count(distinct um.third_type, um.third_id) as payers, sum(pay.rmb)
from 
(
    select ds, region_id, account_id, sum(cast(yuanbao as bigint)/10) as rmb
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds between '20160128' and '20160930'
    group by ds, region_id, account_id
) pay
left outer join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.cn_sbkcq_user_accout_map
    group by region_id, account_id, third_type, third_id
) um
on (pay.region_id = um.region_id and pay.account_id = um.account_id)
group by pay.ds
order by pay.ds;

---- 按照月份充值人数、金额
select substring(pay.ds, 1, 6) as mon, count(distinct um.third_type, um.third_id) as payers, sum(pay.rmb)
from 
(
    select ds, region_id, account_id, sum(cast(yuanbao as bigint)/10) as rmb
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds between '20160128' and '20160930'
    group by ds, region_id, account_id
) pay
left outer join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.cn_sbkcq_user_accout_map
    group by region_id, account_id, third_type, third_id
) um
on (pay.region_id = um.region_id and pay.account_id = um.account_id)
group by substring(pay.ds, 1, 6)
order by mon;

---- 每月分渠道充值数
select substring(t1.ds, 1, 6), t2.third_string, sum(cast(t1.yuanbao as bigint) / 10)
from
(
    select ds, region_id, account_id, sum(yuanbao) as yuanbao
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds between '20160128' and '20160930'
    group by ds, region_id, account_id
) t1
join
(
    select region_id, account_id, third_string, third_id
    from db_stat_sbkcq.cn_sbkcq_user_accout_map
    group by region_id, account_id, third_string, third_id
) t2
on(t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by substring(t1.ds, 1, 6), t2.third_string;


---- 每月按平台充值流水
select ds, substring(ds, 1, 6) as mon, 
    case substring(region_id, 1, 1)
        when '1' then 'android'
        when '2' then 'ios'
        when '3' then 'ios越狱'
    end as platform,
    sum(cast(yuanbao as bigint) / 10) as rmb
from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
where ds between '20160128' and '20160930'
group by ds, substring(ds, 1, 6), 
    case substring(region_id, 1, 1)
        when '1' then 'android'
        when '2' then 'ios'
        when '3' then 'ios越狱'
    end
order by ds, mon, platform;

select substring(t1.ds, 1, 6), substring(t1.region_id, 1, 1), t2.third_string, sum(cast(t1.yuanbao as bigint) / 10)
from
(
    select ds, region_id, account_id, sum(yuanbao) as yuanbao
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds between '20160128' and '20160930'
    group by ds, region_id, account_id
) t1
join
(
    select region_id, account_id, third_string, third_id
    from db_stat_sbkcq.cn_sbkcq_user_accout_map
    group by region_id, account_id, third_string, third_id
) t2
on(t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by substring(t1.ds, 1, 6), substring(t1.region_id, 1, 1), t2.third_string;


---- 每月USER的次日，7日和14日留存
select substring(t1.reg_time, 1, 7), datediff(t2.logindate, substring(t1.reg_time, 1, 10)), count(distinct t1.third_type, t1.third_id)
from
(
    select third_type, third_id, min(reg_time) as reg_time
    from db_stat_sbkcq.cn_sbkcq_user_accout_map
    where ds = '20160630' and substring(reg_time, 1, 7) = '2016-06'
    group by third_type, third_id
) t1
join
(
    select i1.logindate, i2.third_type, i2.third_id
    from
    (
        select substring(time, 1, 10) as logindate, region_id, account_id
        from db_stat_sbkcq.gaea_cn_sbkcq_login_log
        where ds <= '20160716'
        group by substring(time, 1, 10), region_id, account_id
    ) i1
    join
    (
        select region_id, account_id, third_type, third_id
        from db_stat_sbkcq.cn_sbkcq_user_accout_map
        where ds = '20160630'
    ) i2
    on (i1.region_id = i2.region_id and i1.account_id = i2.account_id)
) t2
on (t1.third_type = t2.third_type and t1.third_id = t2.third_id)
where (datediff(t2.logindate, substring(t1.reg_time, 1, 10)) in ('1', '6', '13'))
group by substring(t1.reg_time, 1, 7), datediff(t2.logindate, substring(t1.reg_time, 1, 10));
