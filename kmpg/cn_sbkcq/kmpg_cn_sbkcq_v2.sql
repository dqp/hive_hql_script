---- 截止2016年9月30日，有过合服的日期有：'20160508','20160510','20160516','20160615','20160712','20160718','20160808','20160816','20160912','20161031'


## 重要玩家数据活跃天数
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
    where ds between '20160128' and '20160930'
    group by ds, region_id, account_id
) t3
on (t2.region_id = t3.region_id and t2.account_id = t3.account_id)
group by t1.third_id, t1.third_type, t1.rmb;


## 重要玩家各类型道具消耗总额
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
    where ds between '20160128' and '20160930' 
      and container_type = '3'
    group by region_id, account_id, cmd_id
) t3
on (t2.region_id = t3.region_id and t2.account_id = t3.account_id)
group by t3.cmd_id;

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
    where ds between '20160128' and '20160930' and container_type = '3'
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
