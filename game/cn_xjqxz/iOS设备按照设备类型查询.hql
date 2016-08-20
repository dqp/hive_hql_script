---- iOS设备按照设备类型查询角色留存数据
SET begin_date='20160818';
SET end_date='20160819'; 

select tmp.registerdate as `注册日起`, tmp.devicetype as `设备类型`, 
    max(case when tmp.days = 0 then tmp.rolenum else 0 end) as `角色注册数`,
    max(case when tmp.days = 1 then tmp.rolenum else 0 end) as `次日留存`,
    max(case when tmp.days = 2 then tmp.rolenum else 0 end) as `第3日留存`,
    max(case when tmp.days = 3 then tmp.rolenum else 0 end) as `第4日留存`,
    max(case when tmp.days = 4 then tmp.rolenum else 0 end) as `第5日留存`,
    max(case when tmp.days = 5 then tmp.rolenum else 0 end) as `第6日留存`,
    max(case when tmp.days = 6 then tmp.rolenum else 0 end) as `第7日留存`
from
(
    select
    (
        case 
            when device.devicetype = '' then 'unkown device'
            when device.devicetype = 'NULL' then 'unkown device'
            when device.devicetype is NULL then 'unkown device'
            else device.devicetype
        end
    ) as devicetype,
    register.registerdate,
    datediff(active.activedate, register.registerdate) as days, count(distinct active.serverid, active.accountid) as rolenum
    from
    (
        select from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') as registerdate, deviceid, serverid, accountid
        from xjqxz.gaea_sdk_rolelogin
        where ds between ${hiveconf:begin_date} and ${hiveconf:end_date}
            and appid = 'cn.xjqxz'
            and lower(platform) = 'ios'
            and action = '0'
        group by ds, deviceid, serverid, accountid
    ) register
    left outer join
    (
        select from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') as activedate, serverid, accountid
        from xjqxz.gaea_sdk_rolelogin
        where ds between ${hiveconf:begin_date} and ${hiveconf:end_date}
            and appid = 'cn.xjqxz'
            and lower(platform) = 'ios'
            and serverid is not null
            and accountid is not null
        group by ds, serverid, accountid
    ) active
    on (register.serverid = active.serverid and register.accountid = active.accountid)
    left outer join
    (
        select deviceid, regexp_extract(devicetype, 'iP[a-zA-z]+', 0) devicetype
        from xjqxz.gaea_sdk_startup
        where ds between ${hiveconf:begin_date} and ${hiveconf:end_date}
            and lower(platform) = 'ios'
        group by deviceid, regexp_extract(devicetype, 'iP[a-zA-z]+', 0)
    ) device
    on (register.deviceid = device.deviceid)
    where (active.activedate >= register.registerdate)
    group by device.devicetype, register.registerdate, datediff(active.activedate, register.registerdate)
) tmp
group by tmp.registerdate, tmp.devicetype;



---- iOS设备按照设备类型付费查询
SET ds_date='20160818';
SET scope_date='20160816'; 

select if(device.devicetype is NULL, '未知设备', device.devicetype) as devicetype, count(distinct payment.gameregion, payment.roleid) as rolenum, sum(payment.currencyamount) as rmb
from
(
    select deviceid, gameregion, roleid, orderid, currencyamount 
    from xjqxz.gaea_s_currency
    where ds = ${hiveconf:ds_date}
        and appid = 'cn.xjqxz'
        and lower(platform) = 'ios'
        and orderstatus = '0'
    group by deviceid, gameregion, roleid, orderid, currencyamount
) payment
left join
(
    select deviceid, regexp_extract(devicetype, 'iP[a-zA-z]+', 0) devicetype
    from xjqxz.gaea_sdk_startup
    where ds between ${hiveconf:scope_date} and ${hiveconf:ds_date}
        and lower(platform) = 'ios'
    group by deviceid, regexp_extract(devicetype, 'iP[a-zA-z]+', 0)
) device
on (payment.deviceid = device.deviceid)
group by device.devicetype;