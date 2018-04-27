-- as the top decline category in discount is banana, what to know it if because of shifting to market or totally out
-- start date:20180419 Wanting
-- first update the file for all discount cross shop customer, who banana as top decline

--check the banana customer list
select top 10 *
from DLSDM_MKTD.banana_list;


CREATE MULTISET TABLE DLSDM_MKTD.P20180419wt_banana, no fallback AS  
( select  a.mbrship_id, 
extract(year from trans_dt) as Yr, sh.cust_grp_cd,  
ah.MCH_0_CD, ah.MCH_0_desc_e, 
sum(b.SL_AMT) AS SALES, 
sum(case when b.SL_AMT = 0 or b.SL_QTY > 1000 then 0 else (b.SL_QTY *  (abs(b.SL_AMT)/b.SL_AMT )) end) as Units,
count(distinct b.TRANS_ID) as Txn
	
--only look at the customer sales  info for the one on our list
from DLSDM_MKTD.banana_list a
inner join RLDMPROD_V.PCPLUS_SL_TRANS_LINE_DLY b
on a.mbrship_id = b.mbrship_id

inner join   RLDMPROD_V.SITE_HIER sh
on b.str_site_num =  sh.site_num
and sh.rgn_num = '0003'
and sh.cust_grp_cd in('MK','DS')  -- in ('MK', 'DS', 'FR') 

inner join RLDMPROD_V.CAL_DT dt
on b.trans_dt = dt.cal_dt

inner join  RLDMPROD_V.ARTCL_MCH_HIER_all ah
on b.artcl_num = ah.artcl_num
and ah.MCH_0_desc_e = 'Bananas' 

where b.mbrship_id is not null 
and b.trans_dt between '2015-01-04' and '2017-12-30' 

group by 1,2,3,4,5

) with data PRIMARY INDEX  (mbrship_id, Yr, cust_grp_cd, MCH_0_CD);

select top 10 *
from DLSDM_MKTD.P20180419wt_banana;

select count(mbrship_id) from DLSDM_MKTD.P20180419wt_banana;


