---- 截止2016年9月30日，有过合服的日期有：'20160508','20160510','20160516','20160615','20160712','20160718','20160808','20160816','20160912','20161031'
---- 重要玩家是累计付费金额大于5000


---- 付费玩家
insert overwrite table kp_gaea_audit.cn_sbkcq_users_payment_201609
select t2.third_id, t2.third_type, sum(cast(t1.yuanbao as bigint) / 10) as rmb
from
(
    select ds, region_id, account_id, sum(yuanbao) as yuanbao
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds between '20160128' and '20160930'
    group by ds, region_id, account_id
) t1
left outer join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.cn_sbkcq_user_accout_map
    group by region_id, account_id, third_type, third_id
) t2
on(t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by t2.third_id, t2.third_type;



---- 重要玩家 + 注册 + rmb
select ku.third_type, ku.third_id, ku.rmb, register.registerdate
from
(
    select third_type, third_id, rmb
    from kp_gaea_audit.cn_sbkcq_users_payment_201609
    where cast(rmb as double) > 5000
) ku
left outer join
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
) register
on (ku.third_type = register.third_type and ku.third_id = register.third_id);



---- 重要玩家数据活跃天数
select ku.third_type, ku.third_id, ku.rmb, count(distinct active.ds) as activedays
from
(
    select third_type, third_id, rmb
    from kp_gaea_audit.cn_sbkcq_users_payment_201609
    where cast(rmb as double) > 5000
) ku
left outer join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.cn_sbkcq_user_accout_map
    group by region_id, account_id, third_type, third_id
) um
on (ku.third_type = um.third_type and ku.third_id = um.third_id)
left outer join
(
    select ds, region_id, account_id
    from db_stat_sbkcq.gaea_cn_sbkcq_login_log
    where ds between '20160128' and '20160930'
    group by ds, region_id, account_id
) active
on (um.region_id = active.region_id and um.account_id = active.account_id)
group by ku.third_type, ku.third_id, ku.rmb;


---- 重要玩家各类型道具消耗总额
select t1.third_type, t1.third_id, t1.rmb, t3.cmd_id, sum(t3.yuanbaos) as yuanbaos
from
(
    select third_type, third_id, rmb
    from kp_gaea_audit.cn_sbkcq_users_payment_201609
    where cast(rmb as double) > 5000
    group by third_type, third_id, rmb
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.cn_sbkcq_user_accout_map
    group by region_id, account_id, third_type, third_id
) t2
on (t1.third_type = t2.third_type and t1.third_id = t2.third_id)
join
(
    select region_id, account_id, cmd_id, sum(cast(num as bigint)) as yuanbaos
    from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
    where ds between '20160128' and '20160930'
        and container_type = '3'
        and cast(num as bigint) < 0
        and cmd_id not in ('211', '213', '1733')
    group by region_id, account_id, cmd_id
    union all
    select region_id, account_id, concat(cmd_id, '_', 'tax') as cmd_id, sum(if(cast(num as bigint) * 0.03 > 150, 150, floor(cast(num as bigint) * 0.03))) as yuanbaos
    from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
    where ds between '20160128' and '20160930'
        and container_type = '3'
        and cmd_id in ('213', '1733')
    group by region_id, account_id, concat(cmd_id, '_', 'tax')
) t3
on (t2.region_id = t3.region_id and t2.account_id = t3.account_id)
group by t1.third_type, t1.third_id, t1.rmb, t3.cmd_id
order by t1.third_type, t1.third_id, t1.rmb, t3.cmd_id;


---- 重要玩家每个玩家各类型道具消耗总额
select t1.third_id, t1.third_type, t1.rmb, t3.cmd_id, sum(t3.yuanbaos)
from
(
    select third_id, third_type, rmb
    from kp_gaea_audit.cn_sbkcq_key_users_type
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.cn_sbkcq_user_accout_map
    group by region_id, account_id, third_type, third_id
) t2
on (t1.third_type = t2.third_type and t1.third_id = t2.third_id)
join
(
    select region_id, account_id, cmd_id, sum(cast(num as bigint)) as yuanbaos
    from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
    where ds between '20160128' and '20160930' and container_type = '3'
    group by region_id, account_id, cmd_id
) t3
on (t2.region_id = t3.region_id and t2.account_id = t3.account_id)
group by t1.third_id, t1.third_type, t1.rmb, t3.cmd_id;



---- 重要玩家元宝剩余
select t1.third_id, t1.third_type, t1.rmb, sum(t3.total_num)
from
(
    select third_id, third_type, rmb
    from kp_gaea_audit.cn_sbkcq_key_users_type
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.cn_sbkcq_user_accout_map
    group by region_id, account_id, third_type, third_id
) t2
on (t1.third_type = t2.third_type and t1.third_id = t2.third_id)
join
(
    select i1.region_id, i1.account_id, i2.total_num
    from
    (
        select region_id, account_id, max(time) as time
        from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
        where ds between '20160128' and '20160930' and container_type = '3'
        group by region_id, account_id
    ) i1
    join
    (
        select region_id, account_id, time, total_num
        from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
        where ds between '20160128' and '20160930' and container_type = '3' 
    ) i2
    on (i1.region_id = i2.region_id and i1.account_id = i2.account_id and i1.time = i2.time)
) t3
on (t2.region_id = t3.region_id and t2.account_id = t3.account_id)
group by t1.third_id, t1.third_type, t1.rmb;
