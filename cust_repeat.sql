-- Step 1: check imported table AHList = list of AH provided by AM  
select count(1) from DLSDM_MKTD.HMR_article_list;

select * from DLSDM_MKTD.HMR_article_list;

select * from RLDMPROD_V.ARTCL_AH_HIER;
-- 
collect stats column (Article) on  DLSDM_MKTD.HMR_article_list;

-- Get articles needed based on the AHList provided
create multiset volatile table AHArtcl, no log, no fallback as
(
select distinct ARTCL_NUM, AH_code
from DLSDM_MKTD.HMR_article_list ah inner join 
RLDMPROD_V.ARTCL_AH_HIER al
on ah.Article = al.ARTCL_NUM -- need to chance to real field name
)with data on commit preserve rows;
       
collect stats column(Artcl_Num) on AHArtcl;


-- now that we have the articles and AH_code, from here is very similar to original FriedChicked code, change "group" field to AH_code. 
-- !!!!!!!!! don't forget to change table names!!!!!!!!!!!!!!

drop table DLSDM_MKTD.P20180426_HMR_lastTrans ;

CREATE MULTISET TABLE DLSDM_MKTD.P20180426_HMR_lastTrans, no fallback AS  
(
select 
--s.rgn_num
a.mbrship_id,
s.cust_grp_cd, ah.AH4_CategoryName,ah.AH5_Sub_CategoryName, 
max(trans_dt) as LastTrans

FROM  RLDMPROD_V.PCPLUS_SL_TRANS_LINE_DLY a
inner join  RLDMPROD_V.SITE_HIER s
on a.str_site_num = s.site_num
and s.cust_grp_cd in ('MK', 'FR') -- 'DS'

inner join   DLSDM_MKTD.HMR_article_list ah
on a.artcl_num = ah.Article

where a.trans_dt  between '2017-01-01' and '2017-12-30'
and a.mbrship_id is not null
       
group by 1,2,3,4 
) with data PRIMARY INDEX (cust_grp_cd, mbrship_id, AH4_CategoryName,AH5_Sub_CategoryName);

collect stats column (cust_grp_cd, mbrship_id, AH4_CategoryName,AH5_Sub_CategoryName) on DLSDM_MKTD.P20180426_HMR_lastTrans;
collect stats column (GroupName) on DLSDM_MKTD.P20180416_FrCh_lastTrans;

select count(1) from DLSDM_MKTD.P20180426_HMR_lastTrans;

--7,545,808

select count(1) from DLSDM_MKTD.P20180426_HMR_lastTrans where LastTrans < '2017-05-01';
-- 1,815,681

select * from DLSDM_MKTD.P20180426_HMR_lastTrans order by 4



-- get   180 info
drop table DLSDM_MKTD.P20180426_HMR_lastTrans_repeat180;

CREATE MULTISET TABLE DLSDM_MKTD.P20180426_HMR_lastTrans_repeat180
, no fallback AS  
(
select tr.cust_grp_cd,  
a.mbrship_id,  
ah.AH4_CategoryName , 
count(distinct a.trans_dt || a.str_site_num || a.lane_num || a.trans_num) as Txn
                                                                  
FROM  RLDMPROD_V.PCPLUS_SL_TRANS_LINE_DLY a 
inner join  RLDMPROD_V.SITE_HIER s
on a.str_SITE_NUM = s.site_num
and s.cust_grp_cd in ( 'MK','FR')

inner join DLSDM_MKTD.P20180426_HMR_lastTrans tr
on a.mbrship_id = tr.mbrship_id
--and s.rgn_num = tr.rgn_num
and s.cust_grp_cd = tr.cust_grp_cd 

inner join  DLSDM_MKTD.HMR_article_list ah
on a.artcl_num = ah.Article
and tr.AH4_CategoryName = ah.AH4_CategoryName

where a.trans_dt between tr.LastTrans - 180 and tr.LastTrans
 
group by 1,2,3

) with data PRIMARY INDEX ( cust_grp_cd,  mbrship_id, AH4_CategoryName);

 


/*
select *
from DLSDM_MKTD.P20180416_FrCh_lastTrans_repeat180 a
left join DLSDM_MKTD.P20180416_FrCh_lastTrans_repeat360 b
on a.rgn_num = b.rgn_num 
and a.ban_shrt_desc_e = b.ban_shrt_desc_e
and a.mbrship_id = b.mbrship_id

where a.txn = 1 and b.txn <> 1  
-- 1122998
*/

-- get  360
drop table DLSDM_MKTD.P20180426_HMR_lastTrans_repeat360;

CREATE MULTISET TABLE DLSDM_MKTD.P20180426_HMR_lastTrans_repeat360
, no fallback AS  
(
select tr.cust_grp_cd,  
a.mbrship_id,  
ah.AH4_CategoryName , 
count(distinct a.trans_dt || a.str_site_num || a.lane_num || a.trans_num) as Txn
                                                                  
FROM  RLDMPROD_V.PCPLUS_SL_TRANS_LINE_DLY a 
inner join  RLDMPROD_V.SITE_HIER s
on a.str_SITE_NUM = s.site_num
and s.cust_grp_cd in ( 'MK','FR')

inner join DLSDM_MKTD.P20180426_HMR_lastTrans tr
on a.mbrship_id = tr.mbrship_id
--and s.rgn_num = tr.rgn_num
and s.cust_grp_cd = tr.cust_grp_cd 

inner join  DLSDM_MKTD.HMR_article_list ah
on a.artcl_num = ah.Article
and tr.AH4_CategoryName = ah.AH4_CategoryName

where a.trans_dt between tr.LastTrans - 360 and tr.LastTrans
 
group by 1,2,3

) with data PRIMARY INDEX ( cust_grp_cd,  mbrship_id, AH4_CategoryName);

 
select * from DLSDM_MKTD.P20180426_HMR_lastTrans_repeat180 
where mbrship_id = '0010000016'



select distinct rgn_num, rgn_nm_e from RLDMPROD_V.SITE_HIER ;


--combine the results
select 

cust_grp_cd, 
AH4_CategoryName, 
180 as RepeatShop, 
Txn,
count(distinct mbrship_id) as Mbrs
from DLSDM_MKTD.P20180426_HMR_lastTrans_repeat180 
group by 1,2,3,4

union 

select 

cust_grp_cd, 
AH4_CategoryName, 
360 as RepeatShop, 
Txn,
count(distinct mbrship_id) as Mbrs
from DLSDM_MKTD.P20180426_HMR_lastTrans_repeat360 
group by 1,2,3,4


order by 1,2,3,4

