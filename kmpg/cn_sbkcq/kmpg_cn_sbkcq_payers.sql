---- 截止2016年9月30日，有过合服的日期有：'20160508','20160510','20160516','20160615','20160712','20160718','20160808','20160816','20160912','20161031'


---- 付费玩家钻石获得
select substring(ds, 1, 6), track.cmd_id, sum(track.yuanbaos)
from
(
    select t2.third_id, t2.third_type, min(t1.ds) as firstpaydate
    from
    (
        select region_id, account_id, min(ds) as ds
        from db_stat_sbkcq.gaea_cn_sbkcq_bill_yuanbao_log
        where ds between '20160128' and '20160930'
        group by region_id, account_id
    ) t1
    join
    (
        select region_id, account_id, third_type, third_id
        from db_stat_sbkcq.cn_sbkcq_user_accout_map
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
        where ds between '20160128' and '20160930'
            and container_type = '3'
            and cast(num as bigint) > 0
            and cmd_id not in ('211', '213', '1733')
        group by ds, region_id, account_id, cmd_id
    ) y1
    join
    (
        select region_id, account_id, third_type, third_id
        from db_stat_sbkcq.cn_sbkcq_user_accout_map
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
        where ds between '20160128' and '20160930'
        group by region_id, account_id
    ) t1
    join
    (
        select region_id, account_id, third_type, third_id
        from db_stat_sbkcq.cn_sbkcq_user_accout_map
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
        where ds between '20160128' and '20160930'
            and container_type = '3'
            and cast(num as bigint) < 0
            and cmd_id not in ('211', '213', '1733')
        group by ds, region_id, account_id, cmd_id
        union all
        select ds, region_id, account_id, concat(cmd_id, '_', 'tax'), sum(if(cast(num as bigint) * 0.03 > 150, 150, floor(cast(num as bigint) * 0.03)))
        from db_stat_sbkcq.gaea_cn_sbkcq_log_yuanbao
        where ds between '20160128' and '20160930'
            and container_type = '3'
            and cmd_id in ('211', '1733')
        group by ds, region_id, account_id, cmd_id
    ) y1
    join
    (
        select region_id, account_id, third_type, third_id
        from db_stat_sbkcq.cn_sbkcq_user_accout_map
        where ds = '20160630'
        group by region_id, account_id, third_type, third_id
    ) y2
    on (y1.region_id = y2.region_id and y1.account_id = y2.account_id)
) track
on (payment.third_id = track.third_id and payment.third_type = track.third_type)
where (track.ds >= payment.firstpaydate)
group by substring(ds, 1, 6), track.cmd_id;
