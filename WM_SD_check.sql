-- based on the movement thing we saw, check the actual move clients



-- [customer].[uvw_DimBusinessPerson]  use this to link userid to business person id and userid
select top 100* from [DataMart].[customer].[uvw_DimBusinessPerson]; 
-- have 487803 records, and 
select count(distinct BusinessPersonID) from [DataMart].[customer].[uvw_DimBusinessPerson]; 

-- '412740' has businesspersonid '794337'
select * from  [DataMart].[customer].[uvw_DimBusinessPerson]
where UserID = '412740'; 





-- check assetmovement
--[Datamart].[asset].[FactAccountAssetMovement]
--[DataMart].[account].[DimAccount]; to get account sid 1739252
select top 100* from [Datamart].[asset].[FactAccountAssetMovement] 
where AccountSID ='4319070'
; 

-- one account might have multiple AccountSID
select top 100* from [DataMart].[account].[DimAccount]
where AccountNumber ='60006018'; 


-- look at an account txn info
select b.*
from (select * from [DataMart].[account].[DimAccount]
			  where AccountNumber ='60006018') a
inner join [Datamart].[asset].[FactAccountAssetMovement] b
on a.AccountSID = b.AccountSID
order by TransactionDateKey;


select top 100* from [ReportingDW].[act].[AccountAssetMovement]
where AccountNumber = '60006018'
order by TransactionDate;




-- look at clients info
-- try to look at EquityInCAD as well, if this is null, even it's opened acct, we consider as closed.
select top 100* from [ReportingDW].[act].[Account] 
where PrimaryClientID= '215222';

-- check the asset movement 51556936 51641287
-- client wm acct: '30005136' , sd acct: 26913410

--60006018 51746189
select top 100* from [ReportingDW].[act].[AccountEquity_Historical] 
where AccountNumber = '30005136'
order by ProcessingDate desc;

select top 100* from [ReportingDW].[act].[AccountEquity_Historical] 
where AccountNumber = '26913410'
order by ProcessingDate desc;


-- normal asset movement check
select top 100* from [ReportingDW].[act].[AccountAssetMovement]
where AccountNumber = '51556936'
order by TransactionDate;

select top 100* from [ReportingDW].[act].[AccountAssetMovement]
where AccountNumber = '51641287'
order by TransactionDate;

-- very detail asset movement check
select*
from (select * from [DataMart].[account].[DimAccount]
			  where AccountNumber ='51556936') a
inner join [Datamart].[asset].[FactAccountAssetMovement] b
on a.AccountSID = b.AccountSID
order by TransactionDateKey;

select top 100 * from [ReportingDW].[act].[AccountEquity_Historical] where EquityInCAD = 0.0;


-- look at an account txn info
-- this is the asset check
-- check client number 389411	, account number 26960843 51704402
select top 100* from [ReportingDW].[act].[Account]
where PrimaryClientID = '389411';

select a.AccountNumber, a.AccountSID, b.*
from (select * from [DataMart].[account].[DimAccount]
			  where AccountNumber in ('26960843','51704402')) a
inner join [Datamart].[asset].[FactAccountAssetMovement] b
on a.AccountSID = b.AccountSID
where TransferTypeCode ='Ext' -- only look at the asset move in or not, external only, not internal
order by TransactionDateKey;













-- look at client t0, t3, t6, t12's equity
-- only look at client account type, open time, and trend
drop table #account_info;


select distinct  a.AccountNumber, convert(varchar,a.DateTimeCompleted,23) as completed_date,convert(varchar,a.CloseDateTime,23) as close_date,c.STA_Status, a.PrimaryClientID, a.PrimaryClientFullName, a.TypeName,
b.AccountClass,a.AccountDetailType,a.IsWealthManaged, a.EquityInCAD,a.EquityDate,year(DateTimeCompleted) as create_yr
-- rank()over(partition by a.PrimaryClientID order by a.DateTimeCompleted) as rank_join

into #account_info

from [ReportingDW].[act].[Account] a
inner join (select distinct statusID,STA_Status from [ReportingDWStaging].[crm].[Status]) c
on a.AccountStatusID =c.StatusID
inner join (select AccountNumber,AccountClass,AccountType from [ReportingDW].[client].[Account]) b
on a.AccountNumber =b.AccountNumber

WHERE isDummy = 0 
AND IsForInternalUse = 0 
AND CommissionPlan NOT LIKE '%Emp%'
AND (a.AccountNumber LIKE '[2-6]%' OR a.AccountNumber LIKE 'Q%') 
AND OrganizationID IS NULL
AND IsHouse = 0 
AND IsOffset = 0 
AND DateTimeCompleted IS NOT NULL;


-- reshape your data with ranking in second temp table, as first tem will including all incompleted act, so don't waive this step!!!!!!!
drop table #account_info2 ;

select a.*,
(case when close_date is null then floor(datediff(month,completed_date,GETDATE()))
	else floor(datediff(month,completed_date,close_date)) end) as tenure
 ,rank()over(partition by a.PrimaryClientID order by a.completed_date) as rank_join
into #account_info2
from #account_info a
where PrimaryClientID is not null;


select top 100* from #account_info2;


-- get the first join day of the client
select top 100* from #client order by MTD;
drop table #client;

select distinct PrimaryClientID, min(completed_date) as join_date, min(create_yr) as join_yr, max(tenure) as client_tenure,
floor(datediff(month,min(completed_date),GETDATE())) as MTD,
(case when floor(datediff(day,min(completed_date),convert(varchar,GETDATE(),23))) >= 365 then 1
	 else 0 end) as one_yr_ind
 into #client
from #account_info2
group by PrimaryClientID;


select top 100* from #client;
--



--306,386 acct
drop table #temp;

select a.*, b.join_date,b.join_yr,b.one_yr_ind,b.client_tenure,b.MTD,
(case when close_date is null then convert(varchar,GETDATE(),23) else close_date end) as time_end
into #temp
from #account_info2 a
inner join #client b
on a.PrimaryClientID = b.PrimaryClientID;

select top 10* from #temp;
--for client who join after 2011-02-07, there are only 242k
select count(distinct PrimaryClientID) from #temp where join_date > '2011-02-07';

-- look at every clients asset in the first year, then compare the WM stay vs. other to see any difference.
-- but there are only 252642 records??, if remove the time limiated, there are 291,610 client info
drop table #Asset_info_endt12;

select distinct a.PrimaryClientID,count(distinct a.AccountNumber) as act_num_t12, count(*) as txn_time_t12, 
sum(case when b.AssetsIn <> 0  then 1 else 0 end) as AssetsIn_time_t12,
sum(case when Currency ='CAD' then AssetsIn
		 else AssetsIn*b.USDToCADAverageCurrencyConversionRate end) as AssetIn_ttl_t12,
sum(case when Currency ='CAD' then AssetsOut
		 else AssetsOut*b.USDToCADAverageCurrencyConversionRate end) as AssetsOut_ttl_t12,
sum(case when Currency ='CAD' then TotalAssets
		 else TotalAssets*b.USDToCADAverageCurrencyConversionRate end) as TotalAssets_ttl_t12

into #Asset_info_endt12

from #temp a
inner join [ReportingDW].[act].[AccountAssetMovement] b
on a.AccountNumber = b.AccountNumber
and  b.TransactionDate < dateadd(day,365,a.join_date) 

where (b.AssetsIn <> 0 or b.AssetsOut <> 0)
group by a.PrimaryClientID;

-- check the results
select* from #Asset_info_endt12;
-- check 426327 247776 144284
select top 100* from #Asset_info_endt12;

select* from #temp where PrimaryClientID= '426327';
select* from #Asset_info_endt12 where PrimaryClientID= '426327';


-- this table only have first assetmovement day at 2011-02-07!!!!!
select min(TransactionDate)
from  [ReportingDW].[act].[AccountAssetMovement] 
--and (b.AssetsIn <> 0 or b.AssetsOut <> 0)
;





-- check equity change
select top 100* from [ReportingDW].[act].[Account] where AccountNumber = '26407590';

select top 100* from [ReportingDW].[act].[AccountEquity_Historical]
where AccountNumber = '26407590'
and  ProcessingDate >'2013-05-16'

order by ProcessingDate;

select min(ProcessingDate) as mindate, max(ProcessingDate)as maxdate from [ReportingDW].[act].[AccountEquity_Historical]
where AccountNumber = '26407590'
;

select * from ReportingDW.client.accountstatus
where AccountNumber = '26407590' 
order by DateTimeFrom Desc


-- join to get info, get acct equity for client's first year, then add up
select top 100* from #temp;
select top 100* from [ReportingDW].[act].[AccountEquity_Historical];

-- find the first equity date and the last equity date of each account(not client,but based on clients first year)
drop table #equity_records;

select distinct a.AccountNumber, max(ProcessingDate) as one_yr_equity_date, min(ProcessingDate) as first_equity_dt
 into #equity_records
from (select AccountNumber,join_date from #temp where join_yr in ('2018','2019')) a
inner join [ReportingDW].[act].[AccountEquity_Historical] b
on a.AccountNumber = b.AccountNumber
and b.ProcessingDate between join_date and dateadd(year,1,join_date)
group by a.AccountNumber;

--add in the second part, since too long have to put in two query
-- first step,code above gernrate 184452 records

insert into #equity_records

select distinct a.AccountNumber, max(ProcessingDate) as one_yr_equity_date, min(ProcessingDate) as first_equity_dt
 into #equity_records
from (select AccountNumber,join_date from #temp where join_yr in ('2014','2015','2016','2017')) a
inner join [ReportingDW].[act].[AccountEquity_Historical] b
on a.AccountNumber = b.AccountNumber
and b.ProcessingDate between join_date and dateadd(year,1,join_date)
group by a.AccountNumber;


-- check the table, which only have records join during 2014-2019, there are 255,107 in total
select count(*) from #equity_records;
select top 100* from #equity_records;

-- find the historical records we need
drop table #equity_info;

select b.*
 into #equity_info
from #equity_records a
inner join (select ProcessingDate,AccountNumber,EquityInCAD from [ReportingDW].[act].[AccountEquity_Historical] where ProcessingDate >= '2014-01-01')b
on a.AccountNumber = b.AccountNumber
and (b.ProcessingDate = a.one_yr_equity_date or b.ProcessingDate=a.first_equity_dt);

-- second part add in
insert into #equity_info
select b.*
from #equity_records a
inner join (select ProcessingDate,AccountNumber,EquityInCAD from [ReportingDW].[act].[AccountEquity_Historical] where ProcessingDate >= '2014-01-01') b
on a.AccountNumber = b.AccountNumber
and (b.ProcessingDate = a.one_yr_equity_date or b.ProcessingDate=a.first_equity_dt);



select top 100* from [ReportingDW].[act].[AccountEquity_Historical];

-- total 510,027 records
select top 100* from #equity_info;
select count(*) from #equity_info;


--link the records
select c.AccountNumber,c.ProcessingDate as one_yr_date, c.EquityInCAD as one_yr_equity,
d.ProcessingDate as initial_date, d.EquityInCAD as initial_equity

from
(select  a.AccountNumber,a.ProcessingDate, a.EquityInCAD 
from   #equity_info a
inner join (select AccountNumber,max(ProcessingDate) as ProcessingDate from #equity_info group by AccountNumber) b
on a.AccountNumber = b.AccountNumber
and a.ProcessingDate = b.ProcessingDate) c

left join   (select a.AccountNumber,a.ProcessingDate, a.EquityInCAD 
from   #equity_info a
inner join (select AccountNumber,min(ProcessingDate) as ProcessingDate from #equity_info group by AccountNumber) b
on a.AccountNumber = b.AccountNumber
and a.ProcessingDate = b.ProcessingDate) d
on c.AccountNumber = d.AccountNumber;


-- check your answer
select top 100* from #equity_info;

-- AccountNumber	one_yr_equity_date	first_equity_dt
-- 26407590	2014-05-16	2010-12-16
-- 26408477	2012-01-03	2010-12-16













-- this table will give you a roughly an idea of how client making money or not, MWRR(money-weighted rate of return)
select top 100* from [ClientReporting].[dbo].[MWRR];

-- [DataMart].[customer].[uvw_DimBusinessPerson] clients  model will help you as well



-- Aug,6th,2019 
-- validation of the primary client id gain or loss

-- [customer].[uvw_DimBusinessPerson]  use this to link userid to business person id and userid
select top 100* from [DataMart].[customer].[uvw_DimBusinessPerson]; 
-- have 487803 records, and 
select count(distinct BusinessPersonID) from [DataMart].[customer].[uvw_DimBusinessPerson]; 

-- '412740' has businesspersonid '794337'
select * from  [DataMart].[customer].[uvw_DimBusinessPerson]
where UserID = '412740'; 






















-- look at an account txn info
-- this is the asset check
-- check client number 389411	, account number 26960843 51704402
select top 100* from [ReportingDW].[act].[Account]
where PrimaryClientID = '389411';

select a.AccountNumber, a.AccountSID, b.*
from (select * from [DataMart].[account].[DimAccount]
			  where AccountNumber in ('26960843','51704402')) a
inner join [Datamart].[asset].[FactAccountAssetMovement] b
on a.AccountSID = b.AccountSID
where TransferTypeCode ='Ext' -- only look at the asset move in or not, external only, not internal
order by TransactionDateKey;


-- check clients equity change
select top 100* from [DataMart].[customer].[uvw_FactBusinessPersonAssetSummary];


select top 100* 
from [DataMart].[customer].[uvw_FactBusinessPersonAssetSummary]
where ProcessingPeriod = 'm'
order by ProcessingDate;

BusinessPersonID

select top 100* from [DataMart].[customer].[uvw_DimBusinessPerson] where UserID='389411';


--user id is client crm acct id, not primary client id
-- PrimaryClientID = UserID 
-- crm 892401 759319

select a.UserID, a.FirstAccountCompleteDate, b.*
from (select * from [DataMart].[customer].[uvw_DimBusinessPerson]
			   where UserID='389411' ) a
inner join [DataMart].[customer].[uvw_FactBusinessPersonAssetSummary] b
on a.BusinessPersonID = b.BusinessPersonID
where ProcessingPeriod = 'm'
order by ProcessingDate;



