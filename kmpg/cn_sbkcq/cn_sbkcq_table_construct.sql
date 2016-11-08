kp_gaea_audit.cn_sbkcq_users_payment_201609;


CREATE EXTERNAL TABLE `kp_gaea_audit.cn_sbkcq_users_payment_201609`(
  `third_id` string,
  `third_type` string,
  `rmb` string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '`'
STORED AS PARQUET;