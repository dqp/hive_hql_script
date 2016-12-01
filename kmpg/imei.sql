select imeis.imei, logins.appid, logins.channel, logins.gaeaid, logins.serverid, logins.accountid, min(logins.ds) as registerdate, count(distinct logins.ds) as activedays
from
(
    select imei from kp_gaea_imei
) imeis
left outer join
(
    select ds, appid, deviceid, channel, gaeaid, serverid, accountid
    from db_stat_platform.gaea_stat_login
    where ds <= '20160930'
        and lower(platform) = 'android'
) logins
on (imeis.imei = logins.deviceid)
group by imeis.imei, logins.appid, logins.channel, logins.gaeaid, logins.serverid, logins.accountid;




select imeis.imei, pays.appid, pays.channel, pays.gaeaid, pays.serverid, pays.accountid, pays.currencytype, sum(cast(pays.currencyamount as double)) as amount
from
(
    select imei from kp_gaea_imei
) imeis
left outer join
(
    select ds, appid, deviceid, channel, gaeaid, serverid, accountid, currencytype, currencyamount
    from db_stat_platform.gaea_stat_currency
    where ds <= '20160930'
        and lower(platform) = 'android'
        and orderstatus = '0'
) pays
on (imeis.imei = pays.deviceid)
group by imeis.imei, pays.appid, pays.channel, pays.gaeaid, pays.serverid, pays.accountid, pays.currencytype;
