select ad.campaign, ad.platform, ad.ds, datediff(startup.ds, ad.ds) as days, count(distinct ad.device_id) as devices
from
(
    select campaign, platform, device_id, ds
    from db_stat_gad.gad_device_install
    where ds >= '2016-07-18'
        and appid = 'cn.xjqxz'
        and campaign = '3754'
) ad
left outer join
(
    select from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') as ds, deviceid, platform
    from db_stat_platform.gaea_stat_startup
    where ds >= '20160718'
        and appid = 'cn.xjqxz'
    group by ds, deviceid, platform
) startup
on (ad.device_id = startup.deviceid and ad.platform = startup.platform)
where datediff(startup.ds, ad.ds) between 0 and 1
group by ad.campaign, ad.platform, ad.ds, datediff(startup.ds, ad.ds)




select ad.campaign, ad.platform, ad.ds, ad.device_id, userl.gaeaid
from
(
    select campaign, platform, device_id, ds
    from db_stat_gad.gad_device_install
    where ds = '2016-08-26'
        and appid = 'cn.xjqxz'
        and campaign = '3754'
) ad
join
(
    select from_unixtime(unix_timestamp(ds, 'yyyyMMdd'), 'yyyy-MM-dd') as ds, deviceid, platform
    from db_stat_platform.gaea_stat_startup
    where ds = '20160827'
        and appid = 'cn.xjqxz'
    group by ds, deviceid, platform
) startup
on (ad.device_id = startup.deviceid and ad.platform = startup.platform)
left outer join
(
    select deviceid, gaeaid
    from db_stat_platform.gaea_stat_userlogin
    where ds = '20160827'
        and appid = 'cn.xjqxz'
    group by deviceid, gaeaid
) userl
on (startup.deviceid = userl.deviceid)




select ad.campaign, ad.platform, ad.ds, ad.device_id, d.deviceid
from
(
    select campaign, platform, device_id, ds
    from db_stat_gad.gad_device_install
    where ds = '2016-08-26'
        and appid = 'cn.xjqxz'
        and campaign = '3754'
) ad
left outer join
(
    select deviceid
    from db_stat_platform.gaea_stat_all_device
    where ds = '20160826'
        and appid = 'cn.xjqxz'
        and regisday = '1'
    group by deviceid
) d
on (ad.device_id = d.deviceid)