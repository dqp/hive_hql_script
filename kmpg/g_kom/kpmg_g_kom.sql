---- 测试服： 150

----------------------------------------------------------------
--道具名称、描述对应；只取英文
----------------------------------------------------------------
select itemnamelist.itemcode, itemnamelist.itemname, itemdesclist.itemdesc, itemnamelist.hl
from
(
    select substring(title,2,length(title)-1) as itemcode, phrase as itemname, `language` as hl
    from db_game_origin_mysql_backup.kabam_te_string
    where category ='itemName'
        and language = 'en'
        and ds = '2016-08-01'
) itemnamelist
left outer join
(
    select substring(title,2,length(title)-1) as itemcode, phrase as itemdesc, `language` as hl
    from db_game_origin_mysql_backup.kabam_te_string
    where category = 'itemDesc'
        and language = 'en'
        and ds = '2016-08-01'
) itemdesclist
on (itemnamelist.itemcode = itemdesclist.itemcode and itemnamelist.hl = itemdesclist.hl)
order by cast(itemnamelist.itemcode as bigint);

----------------------------------------------------------------
--5.s1 Kabam KOM  分月计 加入玩家数 
--已修正的数据源(db_game_origin_mysql_backup.gaea_g_kom_user_full)
--注：同步的数据是全字段的，但是db_game_g_kom.gaea_g_kom_user是精简字段的
--@2016.11.04
----------------------------------------------------------------
select substring(date_joined,1,7) as joinmonth, count(distinct userid)
from db_game_g_kom.gaea_g_kom_user
where ds = '2016-10-11'
    and substring(date_joined,1,7) between '2016-01' and '2016-09'
group by substring(date_joined,1,7)
order by joinmonth;
--数据对比用 1/2
2016-01	137918
2016-02	102866
2016-03	92847
2016-04	85377
2016-05	89403
2016-06	104218
2016-07	100839
2016-08	107080
2016-09	90852
----------------------------------------------------------------
--5.s2 Kabam KOM 分月计 首登玩家数
--已优选语句逻辑+优化语句逻辑
--@2016.11.04
----------------------------------------------------------------
select from_unixtime(first_login.fds,'yyyy-MM') as joinmonth, count(distinct first_login.user_id)
from 
(
    select userid as user_id, min(unix_timestamp(`date`,'yyyy-MM-dd')) as fds
    from db_game_g_kom.gaea_g_kom_daily_log_data
    group by userid    
) first_login
where from_unixtime(first_login.fds,'yyyy-MM') between '2016-01' and '2016-09'
group by from_unixtime(first_login.fds,'yyyy-MM')
order by joinmonth;
--数据对比用 2/2
2016-01	136301
2016-02	101634
2016-03	92435
2016-04	85685
2016-05	89039
2016-06	104530
2016-07	101288
2016-08	106437
2016-09	90838

----------------------------------------------------------------
--10. 消耗类型汇总 transactiontype itemid itemcount vcurrency
--Kabam 霍比特人 KOM 虚拟币消耗分类汇总 @所有玩家 单位: gem
----------------------------------------------------------------
select useruse.transactiontype as transactiontype, useruse.itemid as itemid, itemshelf.itemname as itemname, itemshelf.itemdesc as itemdesc, useruse.itemcount as itemcount, useruse.vcurrency_use as vcurrency_use
from
(
    select transactiontype, drainid as itemid, sum(cast(itemcount as bigint)) as itemcount, sum(cast(proposedamount as bigint)) as vcurrency_use
    from db_game_g_kom.gaea_g_kom_gem_transactions
    where (ds between '2016-01-01' and '2016-06-30')
        and fullfilled = '1'
        and serverid != '150' 
        and proposedamount != 0
        and transactiontype in ('2','4','14','990')
    group by transactiontype, drainid
    order by vcurrency_use desc
) useruse
left outer join
(
    select itemnamelist.itemcode, itemnamelist.itemname, itemdesclist.itemdesc
    from
    (
        select substring(title,2,length(title)-1) as itemcode, phrase as itemname
        from db_game_origin_mysql_backup.kabam_te_string
        where language = 'en'
            and category ='itemName'
            and ds = '2016-08-01'
    ) itemnamelist
    left outer join
    (
        select substring(title,2,length(title)-1) as itemcode, phrase as itemdesc
        from db_game_origin_mysql_backup.kabam_te_string
        where language = 'en'
            and category = 'itemDesc'
            and ds = '2016-08-01'
    ) itemdesclist
    on (itemnamelist.itemcode = itemdesclist.itemcode)
) itemshelf
on (useruse.itemid = itemshelf.itemcode);

----------------------------------------------------------------
--(+2). (最后一个附加需求)
--仅道具购买 所有玩家，取前30的道具购买
--道具对应表是和运营/研发同学单要的对应这30个的
--具体这些是什么有另外的对应表，不过基本都是宝箱或宝箱类似物。
----------------------------------------------------------------
select transactiontype, drainid as itemid, (
    case
        when drainid='62893' then '20x Tower Pass'
        when drainid='67668' then 'Wheel Master'
        when drainid='67689' then 'Wheel Mini Master I'
        when drainid='67691' then 'Wheel Mini Master III'
        when drainid='67719' then 'Monday Token 100 Pack'
        when drainid='67732' then '50x Alliance Mania'
        when drainid='67753' then '100x Mounted Monday'
        when drainid='67760' then 'Radagast\'s Replicator'
        when drainid='67805' then 'Collecting Mania 100 Pack'
        when drainid='68646' then 'Tauriel\'s Mini Marvel I'
        when drainid='68647' then 'Tauriel\'s Mini Marvel II'
        when drainid='68648' then 'Tauriel\'s Mini Marvel III'
        when drainid='70855' then 'Tauriel\'s Chest'
        when drainid='70891' then 'Marvel Time'
        when drainid='70892' then '100x Marvel Time'
        when drainid='70934' then 'Gold Rush!'
        when drainid='70960' then '100x Gold Rush'
        when drainid='71008' then 'Bounty Hunter'
        when drainid='71044' then 'Bard\'s Mini Marvel I'
        when drainid='71045' then 'Bard\'s Mini Marvel II'
        when drainid='71046' then 'Bard\'s Mini Marvel III'
        when drainid='71159' then '50x Collecting Mania'
        when drainid='71234' then '50x Collecting Mania'
        when drainid='71293' then '50x Collecting Mania'
        when drainid='71405' then '10x Hobbit\'s Lucky Chest'
        when drainid='71420' then '50x Alliance Mania'
        when drainid='71476' then '50x Alliance Mania'
        when drainid='71543' then 'Gold Rush or Mithril'
        when drainid='71557' then '50x Collecting Mania'
        when drainid='71585' then 'Thorin\'s Mini Marvel III'
        when drainid='71591' then '50x Alliance Mania'
        when drainid='71613' then 'Galadriel\'s Delirium'
        when drainid='71614' then '200x Galadriel\'s Delirium'
        when drainid='5200' then 'Alliance Boss Cooldown'
        when drainid='10004' then 'Gollum\'s Tokens Chest'
        else ''
    end
    ) as itemname, sum(cast(itemcount as bigint)) as itemcount, sum(cast(proposedamount as bigint)) as vcurrency_use, 1.0*sum(proposedamount)/sum(itemcount) as unitprice
from db_game_g_kom.gaea_g_kom_gem_transactions
where (ds between '2016-01-01' and '2016-06-30')
    and fullfilled = '1'
    and serverid != '150' 
    and proposedamount != 0
    and transactiontype = '2'
group by transactiontype, drainid
order by vcurrency_use desc
limit 30;

----------------------------------------------------------------
--同上需求，变形1：仅道具购买 重要玩家
----------------------------------------------------------------
select vcuse.transactiontype as transactiontype, vcuse.itemid as itemid, vcuse.itemname as itemname, sum(vcuse.itemcount) as itemcount, sum(vcuse.vcurrency) as vcurrency_use, 1.0*sum(vcuse.vcurrency)/sum(vcuse.itemcount) as unitprice
from
(
    select userid, payamount, gem 
    from kp_gaea_audit.kabam_kom_key_user_gem 
    where cast(payamount as double) > 1000
) keyuser
left outer join
(
    select userid, transactiontype, drainid as itemid, (
    case
        when drainid='62893' then '20x Tower Pass'
        when drainid='67668' then 'Wheel Master'
        when drainid='67689' then 'Wheel Mini Master I'
        when drainid='67691' then 'Wheel Mini Master III'
        when drainid='67719' then 'Monday Token 100 Pack'
        when drainid='67732' then '50x Alliance Mania'
        when drainid='67753' then '100x Mounted Monday'
        when drainid='67760' then 'Radagast\'s Replicator'
        when drainid='67805' then 'Collecting Mania 100 Pack'
        when drainid='68646' then 'Tauriel\'s Mini Marvel I'
        when drainid='68647' then 'Tauriel\'s Mini Marvel II'
        when drainid='68648' then 'Tauriel\'s Mini Marvel III'
        when drainid='70855' then 'Tauriel\'s Chest'
        when drainid='70891' then 'Marvel Time'
        when drainid='70892' then '100x Marvel Time'
        when drainid='70934' then 'Gold Rush!'
        when drainid='70960' then '100x Gold Rush'
        when drainid='71008' then 'Bounty Hunter'
        when drainid='71044' then 'Bard\'s Mini Marvel I'
        when drainid='71045' then 'Bard\'s Mini Marvel II'
        when drainid='71046' then 'Bard\'s Mini Marvel III'
        when drainid='71159' then '50x Collecting Mania'
        when drainid='71234' then '50x Collecting Mania'
        when drainid='71293' then '50x Collecting Mania'
        when drainid='71405' then '10x Hobbit\'s Lucky Chest'
        when drainid='71420' then '50x Alliance Mania'
        when drainid='71476' then '50x Alliance Mania'
        when drainid='71543' then 'Gold Rush or Mithril'
        when drainid='71557' then '50x Collecting Mania'
        when drainid='71585' then 'Thorin\'s Mini Marvel III'
        when drainid='71591' then '50x Alliance Mania'
        when drainid='71613' then 'Galadriel\'s Delirium'
        when drainid='71614' then '200x Galadriel\'s Delirium'
        when drainid='5200' then 'Alliance Boss Cooldown'
        when drainid='10004' then 'Gollum\'s Tokens Chest'
        else ''
    end
    ) as itemname, cast(itemcount as bigint) as itemcount, cast(proposedamount as bigint) as vcurrency
    from db_game_g_kom.gaea_g_kom_gem_transactions
    where ds between '2016-01-01' and '2016-06-30'
        and transactiontype = '2'
        and fullfilled = '1'
        and serverid != '150' 
        and proposedamount != 0
) vcuse 
on (keyuser.userid = vcuse.userid)
group by vcuse.transactiontype, vcuse.itemid, vcuse.itemname
order by vcurrency_use desc
limit 30;

----------------------------------------------------------------
--消耗虚拟币量前30的消耗类型及drainid，用于以上查询找运营/研发要对应
----------------------------------------------------------------
select transactiontype, drainid as itemid, 
    sum(cast(itemcount as bigint)) as itemcount, 
    sum(cast(proposedamount as bigint)) as vcurrency_use, 
    1.0*sum(proposedamount)/sum(itemcount) as unitprice
from db_game_g_kom.gaea_g_kom_gem_transactions
where (ds between '2016-01-01' and '2016-06-30')
    and fullfilled = '1'
    and serverid != '150' 
    and proposedamount != 0
    and transactiontype = '2'
group by transactiontype, drainid
order by vcurrency_use desc
limit 30;

----------------------------------------------------------------
--Kabam 霍比特人 KOM 虚拟币消耗分类汇总 
--@重要玩家 单位: gem
----------------------------------------------------------------
select vcuse.transactiontype as transactiontype, vcuse.itemid as itemid, sum(vcuse.itemcount) as itemcount, sum(vcuse.vcurrency) as vcurrency_use
from
(
    select userid, payamount, gem 
    from kp_gaea_audit.kabam_kom_key_user_gem 
    where cast(payamount as double) > 1000
) keyuser
left outer join
(
    select userid, transactiontype, drainid as itemid, cast(itemcount as bigint) as itemcount, cast(proposedamount as bigint) as vcurrency
    from db_game_g_kom.gaea_g_kom_gem_transactions
    where ds between '2016-01-01' and '2016-06-30'
        and transactiontype in ('2','4','14','990')
        and fullfilled = '1'
        and serverid != '150' 
        and proposedamount != 0
) vcuse 
on (keyuser.userid = vcuse.userid)
group by vcuse.transactiontype, vcuse.drainid
order by vcurrency_use desc;

----------------------------------------------------------------
--Kabam 霍比特人 KOM 虚拟币消耗按账号ID分类汇总 
--@重要玩家
--表头按： userid transactiontype vcurrency
----------------------------------------------------------------
select keyuser.userid as userid, vcuse.transactiontype as transactiontype, sum(vcuse.vcurrency) as vcurrency_use
from
(
    select userid 
    from kp_gaea_audit.kabam_kom_key_user_gem 
    where cast(payamount as double) > 1000
) keyuser
left outer join
(
    select userid, transactiontype, cast(proposedamount as bigint) as vcurrency
    from db_game_g_kom.gaea_g_kom_gem_transactions
    where ds between '2016-01-01' and '2016-06-30'
        and transactiontype in ('2','4','14','990')
        and fullfilled = '1'
        and serverid != '150' 
        and proposedamount != 0
) vcuse 
on (keyuser.userid = vcuse.userid)
group by keyuser.userid, vcuse.transactiontype;

----------------------------------------------------------------
--Kabam 霍比特人 KOM 分月分渠道充值数据 USD
--为结果美观，去掉了美元金额为零的行
----------------------------------------------------------------
select substring(ds,1,7) as thismonth,
    if(flow is null,
        '',
        if(instr(flow,'_')=0,
            flow,
            if(size(split(flow,'_'))=3,
                substring(flow,1,length(flow)-4),
                split(flow,'_')[0]
            )
        )
    ) as channel,
    sum(cast(cents as bigint))/100.0 as amount_usd
from db_game_g_kom.gaea_g_kom_dollar_transactions
where ds between '2016-01-01' and '2016-09-30'
    and serverid != '150'
group by substring(ds,1,7), 
    if(flow is null,
        '',
        if(instr(flow,'_')=0,
            flow,
            if(size(split(flow,'_'))=3,
                substring(flow,1,length(flow)-4),
                split(flow,'_')[0]
            )
        )
    )
having sum(cast(cents as bigint))/100.0 != 0;

----------------------------------------------------------------
--9. 重要玩家消耗明细 
--表头：userid pay_usd vcurrency transactiontype itemid itemcount vcurrency
----------------------------------------------------------------

select keyuser.userid as userid, keyuser.pay_usd as pay_usd, keyuser.vcurrency_get as vcurrency_get, useamount.itemid as itemid, 
    sum(useamount.itemcount) as itemcount, sum(useamount.vcurrency_use) as vcurrency_use
from
(
    select userid, payamount as pay_usd, gem as vcurrency_get
    from kp_gaea_audit.kabam_kom_key_user_gem 
    where cast(payamount as double) > 1000
) keyuser
left outer join
(            
    select userid, drainid as itemid, cast(itemcount as bigint) as itemcount, cast(proposedamount as bigint) as vcurrency_use
    from db_game_g_kom.gaea_g_kom_gem_transactions
    where (ds between '2016-01-01' and '2016-06-30')
        and fullfilled = '1'
        and serverid != '150' 
        and proposedamount != 0
        and transactiontype in ('2','4','14','990')
) useamount
on (keyuser.userid = useamount.userid)
group by keyuser.userid, keyuser.pay_usd, keyuser.vcurrency_get, useamount.itemid;

----------------------------------------------------------------
--Kabam 霍比特人 KOM 重要玩家剩余元宝
--取重要玩家，关联到其计算期间结束日前的最后一次有获得/消耗时，
--通过同步时状态表时间之前，所有交易，逆推回其计算期间结束日的剩余虚拟币数量
----------------------------------------------------------------
select changestatus.userid, changestatus.payamount, changestatus.vcurrency_get, sum(finalstatus.vcurrency - changestatus.vcurrency) as vcurrency_use
from 
(
    select keyuserlast.userid, keyuserlast.payamount, keyuserlast.vcurrency_get, sum(transactionsafter.vcurrency) as vcurrency
    from
    (
        select keyuser.userid, keyuser.payamount, keyuser.vcurrency_get, transactionlastds.lastds
        from
        (
            select userid, payamount, gem as vcurrency_get
            from kp_gaea_audit.kabam_kom_key_user_gem 
            where cast(payamount as double) > 1000
        ) keyuser
        left outer join
        (
            select transactions.userid as userid, max(unix_timestamp(transactions.createddate,'yyyy-MM-dd HH:mm:ss')) as lastds
            from 
            (
                select userid, createddate
                from db_game_g_kom.gaea_g_kom_gem_transactions
                where (ds between '2016-01-01' and '2016-09-30')
                    and fullfilled = '1'
                    and serverid != '150' 
                    and proposedamount != 0
                    
                union all 
                
                select userid, createddate
                from db_game_g_kom.gaea_g_kom_s_world_gem_transactions
                where ds = '2016-10-11'
                    and (substring(createddate,1,10) between '2016-01-01' and '2016-09-30')
                    and fullfilled = '1'
                    and serverid != '150'
                    and proposedamount != 0
                    and transactiontype in ('3','13')                
            ) transactions
            group by transactions.userid
        ) transactionlastds
        on(keyuser.userid = transactionlastds.userid)
    ) keyuserlast
    left outer join
    (
        select trdt.gemtransactionid, trdt.userid as userid, trdt.serverid, 
            if(trdt.transactiontype in ('2','4','14','990'),-trdt.proposedamount,trdt.proposedamount) as vcurrency, trdt.createddate as createddate
        from
        (
            select gemtransactionid, userid, serverid, transactiontype, proposedamount, createddate
            from db_game_g_kom.gaea_g_kom_gem_transactions
            where (ds between '2016-01-01' and '2016-12-31')
                and fullfilled = '1'
                and serverid != '150' 
                and proposedamount != 0
        ) trdt
        join
        (
            select max(unix_timestamp(firstdate,'yyyy-MM-dd HH:mm:ss')) as mdt
            from db_game_origin_mysql_backup.gaea_g_kom_gem_account
            where ds='2016-10-11'
        ) accmdt
        on (true)
        where unix_timestamp(createddate,'yyyy-MM-dd HH:mm:ss') - accmdt.mdt <= 0
        
        union all 
        
        select wtrdt.gemtransactionid, wtrdt.userid as userid, wtrdt.serverid, wtrdt.proposedamount as vcurrency, wtrdt.createddate as createddate
        from
        (
            select gemtransactionid,userid,serverid,proposedamount,createddate
            from db_game_g_kom.gaea_g_kom_s_world_gem_transactions
            where ds = '2016-10-11'
                and (substring(createddate,1,10) between '2016-01-01' and '2016-12-31')
                and fullfilled = '1'
                and serverid != '150'
                and proposedamount != 0
                and transactiontype in ('3','13')
        ) wtrdt
        join
        (
            select max(unix_timestamp(firstdate,'yyyy-MM-dd HH:mm:ss')) as mdt
            from db_game_origin_mysql_backup.gaea_g_kom_s_world_gem_account
            where ds='2016-10-11'
        ) waccmdt
        on (true)
        where unix_timestamp(createddate,'yyyy-MM-dd HH:mm:ss') - waccmdt.mdt <= 0
        group by wtrdt.gemtransactionid, wtrdt.userid, wtrdt.serverid, wtrdt.proposedamount, wtrdt.createddate
        
    ) transactionsafter
    on (keyuserlast.userid = transactionsafter.userid)
    where unix_timestamp(transactionsafter.createddate,'yyyy-MM-dd HH:mm:ss') - keyuserlast.lastds >= 0
    group by keyuserlast.userid, keyuserlast.payamount, keyuserlast.vcurrency_get
) changestatus
left outer join
(    
    select cm.userid, sum(cast(cm.vcurrency as bigint)) as vcurrency
    from
    (
        select userid, availablegems as vcurrency
        from db_game_g_kom.gaea_g_kom_gem_account
        where ds='2016-10-11'
        
        union all
        
        select wGemAcount.userid as userid, wGemAcount.vcurrency as vcurrency
        from
        (
            select userid, availablegems as vcurrency, serverid
            from db_game_g_kom.gaea_g_kom_s_world_gem_account
            where ds='2016-10-11'
                and serverid !='150'
        ) wGemAcount
        left outer join
        (    
            select serverlist.serverid
            from
            (
                select serverid
                from db_game_g_kom.gaea_g_kom_s_world_gem_account
                where ds='2016-10-11'
                    and serverid !='150'
                group by serverid
            ) serverlist
            left outer join
            (
                select sourceserverids
                from db_game_origin_mysql_backup.gaea_g_kom_server_merge_plan
                where ds = '2016-10-11'
                    and sourceserverids is not null
                group by sourceserverids
            ) excludeserverlist
            on (true)
            where find_in_set(serverlist.serverid, excludeserverlist.sourceserverids) != 0
        ) mergeServer
        on(wGemAcount.serverid = mergeServer.serverid)
        where mergeServer.serverid is null
    ) cm
    group by cm.userid
) finalstatus
on (changestatus.userid = finalstatus.userid)
group by changestatus.userid, changestatus.payamount, changestatus.vcurrency_get;


----------------------------------------------------------------
--Kabam 霍比特人 KOM 重要玩家 累计登录总天数
----------------------------------------------------------------
select keyuser.userid, keyuser.payamount, keyuser.gem, count(distinct loginday.ds)
from 
(
    select userid, payamount, gem
    from kp_gaea_audit.kabam_kom_key_user_gem 
    where cast(payamount as double) > 1000
) keyuser
join
(
    select userid, substring(`date`,1,10) as ds
    from db_stat_kabam_mysql.kom_daily_log_data 
    where ds between '2016-01-01' and '2016-09-30'
) loginday
on (keyuser.userid = loginday.userid)
group by keyuser.userid, keyuser.payamount, keyuser.gem
order by keyuser.userid, keyuser.payamount, keyuser.gem;

----------------------------------------------------------------
--Kabam 霍比特人 KOM userid对应付费金额(USD)和充值获得钻石数
----------------------------------------------------------------
--账号付费、获得虚拟币数量>建表，用于取重要玩家
create table kp_gaea_audit.kabam_kom_key_user_gem
(
    userid string,
    payamount string,
    gem string
);
--账号付费、获得虚拟币数量>建表(数据)，用于取重要玩家
insert into kp_gaea_audit.kabam_kom_key_user_gem
select userid, sum(cents)/100.0 as payamount, sum(gems) as gem
from db_game_origin_mysql_backup.gaea_g_kom_dollar_transactions
where ds = '2016-10-11'
    and substring(createddate,1,10) between '2016-01-01' and '2016-09-30'
group by userid;

----------------------------------------------------------------
--KOM 月活跃MAU，按月排重计登录过的账号ID数
----------------------------------------------------------------
select substring(`date`,1,7) as thismonth, count(distinct userid)
from db_game_g_kom.gaea_g_kom_daily_log_data
where ds between '2016-01-01' and '2016-09-30'
group by substring(`date`,1,7)
order by thismonth;
