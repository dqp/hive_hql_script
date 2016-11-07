#!/bin/bash
# 每天新增注册玩家（个）
hive -S -e "
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

select ds, count(distinct third_type, third_id) 
from db_stat_sbkcq.gaea_cn_sbkcq_account_third
where ds between '20160701' and '20160930'
  and substring(reg_time, 1, 10) = from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd')
group by ds  
" > newUserDaily20160930.txt

# 每天活跃用户数（个）
hive -S -e "
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;

select t1.ds, count(distinct t2.third_type, t2.third_id)
from
(
    select ds, region_id, account_id
    from db_stat_sbkcq.gaea_cn_sbkcq_login_log
    where ds between '20160701' and  '20160930'
    group by ds, region_id, account_id
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_third
      where ds = '20160930'
    group by region_id, account_id, third_type, third_id
) t2
on (t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by t1.ds;
" > activeUserDaily20160930.txt

# 每月活跃用户数（个）
hive -S -e "
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
    where ds between '20160701' and  '20160930'
    group by substring(ds, 1, 6), region_id, account_id
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_third
    where ds = '20160930'
    group by region_id, account_id, third_type, third_id
) t2
on (t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by t1.mon;
" > activeUserMonthly20160930.txt

# 每天付费用户数（个）
hive -S -e "
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
select ds , count(distinct t2.third_id, t2.third_type)
from
(
    select region_id, account_id, ds
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds between '20160701' and  '20160930'
    group by region_id, account_id, ds
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_third
    where ds = '20160930'
    group by region_id, account_id, third_type, third_id
) t2
on(t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by ds
" > payersDaily20160930.txt

# 每月付费用户数（个）
hive -S -e "
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
select substring(ds, 1, 6) , count(distinct t2.third_id, t2.third_type)
from
(
    select region_id, account_id, ds
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds between '20160701' and '20160930'
    group by region_id, account_id, ds
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_third
    where ds = '20160930'
    group by region_id, account_id, third_type, third_id
) t2
on(t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by substring(ds, 1, 6)
" > payersMonthly20160930.txt

# 每天充值流水（元）

hive -S -e "
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
select ds, substring(t1.region_id, 1, 1), t2.third_string, sum(cast(t1.yuanbao as bigint) / 10)
from
(
    select ds, region_id, account_id, sum(yuanbao) as yuanbao
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds between '20160701' and '20160930'
    group by ds, region_id, account_id
) t1
join
(
    select region_id, account_id, third_string, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_common
    where ds = '20160930'
    group by region_id, account_id, third_string, third_id
) t2
on(t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by ds, substring(t1.region_id, 1, 1), t2.third_string;
" > paymentDaily20160930.txt

# 每天充值虚拟货币数
hive -S -e "
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
select ds, substring(t1.region_id, 1, 1), t2.third_string, sum(cast(t1.yuanbao as bigint) ) as yuanbao
from
(
    select ds, region_id, account_id, sum(yuanbao) as yuanbao
    from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
    where ds between '20160701' and '20160930'
    group by ds, region_id, account_id
) t1
join
(
    select region_id, account_id, third_string, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_common
    where ds = '20160930'
    group by region_id, account_id, third_string, third_id
) t2
on(t1.region_id = t2.region_id and t1.account_id = t2.account_id)
group by ds, substring(t1.region_id, 1, 1), t2.third_string;
" > yuanbaoChargeDaily20160930.txt

# 每天消耗虚拟货币数
hive -S -e "
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
select ds, substring(tmp.ds, 1, 6), tmp.cmd_id, sum(tmp.yuanbaos)
from
(
    select ds, cmd_id, sum(cast(num as bigint)) as yuanbaos
    from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
    where ds between '20160701' and '20160930'
        and container_type = '3'
        and cast(num as bigint) < 0
        and cmd_id not in ('211', '213', '1733')
    group by ds, cmd_id
    union all
    select ds, concat(cmd_id, '_', 'tax') as cmd_id, sum(if(cast(num as bigint) * 0.03 > 150, 150, floor(cast(num as bigint) * 0.03))) as yuanbaos
    from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
    where ds between '20160701' and '20160930'
        and container_type = '3'
        and cmd_id in ('213', '1733')
    group by ds, cmd_id
) tmp
group by ds, substring(tmp.ds, 1, 6), tmp.cmd_id
" > yuanbaoConsumeDaily20160930.txt







## 重要玩家数据活跃天数
hive -S -e "
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
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
    where ds = '20160930'
    group by region_id, account_id, third_type, third_id
) t2
on (t1.third_type = t2.third_type and t1.third_id = t2.third_id)
join
(
    select ds, region_id, account_id
    from db_stat_sbkcq.gaea_cn_sbkcq_login_log
    where ds between '20160701' and '20160930'
    group by ds, region_id, account_id
) t3
on (t2.region_id = t3.region_id and t2.account_id = t3.account_id)
group by t1.third_id, t1.third_type, t1.rmb;
" > keyUserActiveDays20160930.txt


## 重要玩家各类型道具消耗总额
hive -S -e "
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
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
      where ds = '20160930'
    group by region_id, account_id, third_type, third_id
) t2
on (t1.third_type = t2.third_type and t1.third_id = t2.third_id)
join
(
    select region_id, account_id, cmd_id, sum(cast(num as bigint)) as yuanbaos
    from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
    where ds between '20160701' and '20160930' 
      and container_type = '3'
    group by region_id, account_id, cmd_id
) t3
on (t2.region_id = t3.region_id and t2.account_id = t3.account_id)
group by t3.cmd_id;
" > keyUserItemConsumeSum20160930.txt

## 重要玩家每个玩家各类型道具消耗总额
hive -S -e "
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
select t1.third_id, t1.third_type, t1.rmb, t    3.cmd_id, sum(t3.yuanbaos)
from
(
    select third_id, third_type, rmb
    from kp_gaea_audit.cn_sbkcq_key_users_type
      where ds = '20160930'
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_third
      where ds = '20160930'
    group by region_id, account_id, third_type, third_id
) t2
on (t1.third_type = t2.third_type and t1.third_id = t2.third_id)
join
(
    select region_id, account_id, cmd_id, sum(cast(num as bigint)) as yuanbaos
    from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
    where ds between '20160701' and '20160930' and container_type = '3'
    group by region_id, account_id, cmd_id
) t3
on (t2.region_id = t3.region_id and t2.account_id = t3.account_id)
group by t1.third_id, t1.third_type, t1.rmb, t3.cmd_id;
" > keyUserItemPerTypeSum20160930.txt


## 重要玩家元宝剩余
hive -S -e "
set mapred.reduce.tasks=16;
set hive.exec.reducers.max=10;
set hive.exec.reducers.bytes.per.reducer=500000000;
set hive.exec.max.dynamic.partitions=100000;
set hive.exec.max.dynamic.partitions.pernode=100000;
set mapred.max.split.size=100000000;
set mapred.min.split.size.per.node=100000000;
set mapred.min.split.size.per.rack=100000000;
set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
select t1.third_id, t1.third_type, t1.rmb, sum(t3.total_num)
from
(
    select third_id, third_type, rmb
    from kp_gaea_audit.cn_sbkcq_key_users_type
      where ds = '20160930'
) t1
join
(
    select region_id, account_id, third_type, third_id
    from db_stat_sbkcq.gaea_cn_sbkcq_account_third
      where ds = '20160930'
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
" > keyUserYuanbaoLeft20160930.txt
