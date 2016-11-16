---- 截止2016年9月30日，有过合服的日期有：'20160508','20160510','20160516','20160615','20160712','20160718','20160808','20160816','20160912','20161031'


CREATE EXTERNAL TABLE `kp_gaea_audit.cn_sbkcq_users_payment_201609`(
  `third_id` string,
  `third_type` string,
  `rmb` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS PARQUET;


---- 用户
CREATE EXTERNAL TABLE `db_stat_sbkcq.cn_sbkcq_user_accout_map`(
  `region_id` string,
  `account_id` string,
  `third_type` string,
  `third_id` string,
  `reg_time` string
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS PARQUET;

insert overwrite table db_stat_sbkcq.cn_sbkcq_user_accout_map
select region_id, account_id, third_type, third_id, reg_time
from db_stat_sbkcq.gaea_cn_sbkcq_account_third
where ds in ('20160508','20160510','20160516','20160615','20160712','20160718','20160808','20160816','20160912','20161031')
group by region_id, account_id, third_type, third_id, reg_time;