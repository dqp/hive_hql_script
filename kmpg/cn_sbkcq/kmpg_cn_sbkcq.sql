select t2.from_currency, t2.to_currency, t2.exchange_rate
from
(
    select from_currency, min(date) as date 
    from currency_type_excharge_rate
    where date >= '2016-01-01'
    group by from_currency
) t1
join
(
    select from_currency, to_currency, exchange_rate, date
    from currency_type_excharge_rate
    where date >= '2016-01-01'
) t2
on (t1.from_currency = t2.from_currency and t1.date = t2.date)



---- user月活跃
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
select t1.mon, count(distinct t2.third_type, t2.third_id)
from
(
    select substring(ds, 1, 6) as mon, region_id, account_id
    from db_stat_sbkcq.gaea_cn_sbkcq_login_log
    where ds <= '20160630'
    group by substring(ds, 1, 6), region_id, account_id
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_third
    group by region_id, account_id, third_type, third_id
) t2
on (t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by t1.mon;



---- 每月yuanbao各类型消耗
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

select substring(ds, 1, 6), cmd_id, sum(cast(num as bigint)) as yuanbaos
from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
where ds <= '20160630' and container_type = '3'
group by substring(ds, 1, 6), cmd_id;


---- 每月yuanbao变化
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

---- 获得
select substring(ds, 1, 6), cmd_id, sum(cast(num as bigint)) as yuanbaos
from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
where ds <= '20160630'
    and container_type = '3'
    and cast(num as bigint) > 0
    and cmd_id not in ('211', '213', '1733')
group by substring(ds, 1, 6), cmd_id;


---- 消耗
select substring(tmp.ds, 1, 6), tmp.cmd_id, sum(tmp.yuanbaos)
from
(
    select ds, cmd_id, sum(cast(num as bigint)) as yuanbaos
    from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
    where ds <= '20160630'
        and container_type = '3'
        and cast(num as bigint) < 0
        and cmd_id not in ('211', '213', '1733')
    group by ds, cmd_id
    union all
    select ds, concat(cmd_id, '_', 'tax') as cmd_id, sum(if(cast(num as bigint) * 0.03 > 150, 150, floor(cast(num as bigint) * 0.03))) as yuanbaos
    from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
    where ds <= '20160630'
        and container_type = '3'
        and cmd_id in ('213', '1733')
    group by ds, cmd_id
) tmp
group by substring(tmp.ds, 1, 6), tmp.cmd_id

---- 每月USER的次日，7日和14日   留存
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
select substring(t1.reg_time, 1, 7), datediff(t2.logindate, substring(t1.reg_time, 1, 10)), count(distinct t1.third_type, t1.third_id)
from
(
    select third_type, third_id, min(reg_time) as reg_time
    from db_stat_sbkcq.gaea_cn_sbkcq_account_third
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
        from db_stat_sbkcq.gaea_cn_sbkcq_account_third
        where ds = '20160630'
    ) i2
    on (i1.region_id = i2.region_id and i1.account_id = i2.account_id)
) t2
on (t1.third_type = t2.third_type and t1.third_id = t2.third_id)
where (datediff(t2.logindate, substring(t1.reg_time, 1, 10)) in ('1', '6', '13'))
group by substring(t1.reg_time, 1, 7), datediff(t2.logindate, substring(t1.reg_time, 1, 10));




---- 每月分渠道充值数
select substring(t1.ds, 1, 6), t2.third_string, sum(cast(t1.yuanbao as bigint) / 10)
from
(
    select ds, region_id, account_id, sum(yuanbao) as yuanbao
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds <= '20160630'
    group by ds, region_id, account_id
) t1
join
(
    select region_id, account_id, third_string, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_common
    where ds = '20160630'
    group by region_id, account_id, third_string, third_id
) t2
on(t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by substring(t1.ds, 1, 6), t2.third_string;

---- 每月分渠道充值数(带平台)
select substring(t1.ds, 1, 6), substring(t1.region_id, 1, 1), t2.third_string, sum(cast(t1.yuanbao as bigint) / 10)
from
(
    select ds, region_id, account_id, sum(yuanbao) as yuanbao
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds <= '20160630'
    group by ds, region_id, account_id
) t1
join
(
    select region_id, account_id, third_string, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_common
    where ds = '20160630'
    group by region_id, account_id, third_string, third_id
) t2
on(t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by substring(t1.ds, 1, 6), substring(t1.region_id, 1, 1), t2.third_string;


---- 重要玩家数据
insert overwrite table kp_gaea_audit.cn_sbkcq_key_users
select t2.third_id, t2.third_string, sum(cast(t1.yuanbao as bigint) / 10) as rmb
from
(
    select ds, region_id, account_id, sum(yuanbao) as yuanbao
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds <= '20160630'
    group by ds, region_id, account_id
) t1
join
(
    select region_id, account_id, third_string, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_common
    where ds = '20160630'
    group by region_id, account_id, third_string, third_id
) t2
on(t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by t2.third_id, t2.third_string
having rmb > 5000;



---- 重要玩家 + 注册 + rmb
insert overwrite table kp_gaea_audit.cn_sbkcq_key_users_type
select t2.third_id, t2.third_type, min(t2.reg_time), sum(cast(t1.yuanbao as bigint) / 10) as rmb
from
(
    select ds, region_id, account_id, sum(cast(yuanbao as bigint)) as yuanbao
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds <= '20160630'
    group by ds, region_id, account_id
) t1
join
(
    select region_id, account_id, third_type, third_id, reg_time
    from db_stat_sbkcq.gaea_cn_sbkcq_account_third
    where ds = '20160630'
    group by region_id, account_id, third_type, third_id, reg_time
) t2
on(t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by t2.third_id, t2.third_type
having rmb > 5000;


---- 重要玩家数据活跃天数
select t1.third_id, t1.third_type, t1.rmb, count(distinct t3.ds)
from
(
    select third_id, third_type, rmb
    from kp_gaea_audit.cn_sbkcq_key_users_type
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_third
    where ds = '20160630'
    group by region_id, account_id, third_type, third_id
) t2
on (t1.third_type = t2.third_type and t1.third_id = t2.third_id)
join
(
    select ds, region_id, account_id
    from db_stat_sbkcq.gaea_cn_sbkcq_login_log
    where ds <= '20160630'
    group by ds, region_id, account_id
) t3
on (t2.region_id = t3.region_id and t2.account_id = t3.account_id)
group by t1.third_id, t1.third_type, t1.rmb;



---- 重要玩家各类型道具消耗总额
select t3.cmd_id, sum(t3.yuanbaos)
from
(
    select third_id, third_type, rmb
    from kp_gaea_audit.cn_sbkcq_key_users_type
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_third
    group by region_id, account_id, third_type, third_id
) t2
on (t1.third_type = t2.third_type and t1.third_id = t2.third_id)
join
(
    select region_id, account_id, cmd_id, sum(cast(num as bigint)) as yuanbaos
    from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
    where ds <= '20160630' and container_type = '3'
    group by region_id, account_id, cmd_id
) t3
on (t2.region_id = t3.region_id and t2.account_id = t3.account_id)
group by t3.cmd_id;


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
    from db_stat_sbkcq.gaea_cn_sbkcq_account_third
    group by region_id, account_id, third_type, third_id
) t2
on (t1.third_type = t2.third_type and t1.third_id = t2.third_id)
join
(
    select region_id, account_id, cmd_id, sum(cast(num as bigint)) as yuanbaos
    from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
    where ds <= '20160630' and container_type = '3'
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
    from db_stat_sbkcq.gaea_cn_sbkcq_account_third
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
        where ds <= '20160630' and container_type = '3'
        group by region_id, account_id
    ) i1
    join
    (
        select region_id, account_id, time, total_num
        from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
        where ds <= '20160630' and container_type = '3' 
    ) i2
    on (i1.region_id = i2.region_id and i1.account_id = i2.account_id and i1.time = i2.time)
) t3
on (t2.region_id = t3.region_id and t2.account_id = t3.account_id)
group by t1.third_id, t1.third_type, t1.rmb;




---- 付费玩家钻石获得
select substring(ds, 1, 6), track.cmd_id, sum(track.yuanbaos)
from
(
    select t2.third_id, t2.third_type, min(t1.ds) as firstpaydate
    from
    (
        select region_id, account_id, min(ds) as ds
        from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
        where ds <= '20160630'
        group by region_id, account_id
    ) t1
    join
    (
        select region_id, account_id, third_type, third_id
        from db_stat_sbkcq.gaea_cn_sbkcq_account_third
        where ds = '20160630'
        group by region_id, account_id, third_type, third_id
    ) t2
    on(t1.region_id = t2.region_id and t1.account_id = t2.account_id)
    group by t2.third_id, t2.third_type
) payment
join
(
    select y2.third_type, y2.third_id, y1.ds, y1.cmd_id, y1.yuanbaos
    from
    (
        select ds, region_id, account_id, cmd_id, sum(cast(num as bigint)) as yuanbaos
        from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
        where ds <= '20160630'
            and container_type = '3'
            and cast(num as bigint) > 0
            and cmd_id not in ('211', '213', '1733')
        group by ds, region_id, account_id, cmd_id
    ) y1
    join
    (
        select region_id, account_id, third_type, third_id
        from db_stat_sbkcq.gaea_cn_sbkcq_account_third
        where ds = '20160630'
        group by region_id, account_id, third_type, third_id
    ) y2
    on (y1.region_id = y2.region_id and y1.account_id = y2.account_id)
) track
on (payment.third_id = track.third_id and payment.third_type = track.third_type)
where (track.ds >= payment.firstpaydate)
group by substring(ds, 1, 6), track.cmd_id;


---- 付费玩家钻石消耗
select substring(ds, 1, 6), track.cmd_id, sum(track.yuanbaos)
from
(
    select t2.third_id, t2.third_type, min(t1.ds) as firstpaydate
    from
    (
        select region_id, account_id, min(ds) as ds
        from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
        where ds <= '20160630'
        group by region_id, account_id
    ) t1
    join
    (
        select region_id, account_id, third_type, third_id
        from db_stat_sbkcq.gaea_cn_sbkcq_account_third
        where ds = '20160630'
        group by region_id, account_id, third_type, third_id
    ) t2
    on(t1.region_id = t2.region_id and t1.account_id = t2.account_id)
    group by t2.third_id, t2.third_type
) payment
join
(
    select y2.third_type, y2.third_id, y1.ds, y1.cmd_id, y1.yuanbaos
    from
    (
        select ds, region_id, account_id, cmd_id, sum(cast(num as bigint)) as yuanbaos
        from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
        where ds <= '20160630'
            and container_type = '3'
            and cast(num as bigint) < 0
            and cmd_id not in ('211', '213', '1733')
        group by ds, region_id, account_id, cmd_id
        union all
        select ds, region_id, account_id, concat(cmd_id, '_', 'tax'), sum(if(cast(num as bigint) * 0.03 > 150, 150, floor(cast(num as bigint) * 0.03)))
        from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
        where ds <= '20160630'
            and container_type = '3'
            and cmd_id in ('211', '1733')
        group by ds, region_id, account_id, cmd_id
    ) y1
    join
    (
        select region_id, account_id, third_type, third_id
        from db_stat_sbkcq.gaea_cn_sbkcq_account_third
        where ds = '20160630'
        group by region_id, account_id, third_type, third_id
    ) y2
    on (y1.region_id = y2.region_id and y1.account_id = y2.account_id)
) track
on (payment.third_id = track.third_id and payment.third_type = track.third_type)
where (track.ds >= payment.firstpaydate)
group by substring(ds, 1, 6), track.cmd_id;

