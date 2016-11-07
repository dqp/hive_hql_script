CREATE EXTERNAL TABLE `db_game_web_wt.gaea_web_wt_charge`
(
    `orderid` string,
    `roleid` string,
    `udid` string,
    `rolename` string,
    `paychannel` string,
    `currency` string,
    `diamonds` string,
    `currencytype` string,
    `time` string,
    `serverid` string
)PARTITIONED BY
(
    ds string
);

ds=from_unixtime(unix_timestamp(`time`,'yyyy-MM-dd HH:mm:ss'),'yyyy-MM-dd')