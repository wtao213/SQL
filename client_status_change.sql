-- try to look at every client acct type trend by time

select top 100* from [ReportingDW].[act].[Account]
;

-- July 22th, 2019
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


-- verify your pull
select top 100* from #account_info;

select * from #account_info where rank_join =2;

select * from #account_info where PrimaryClientID = '119171';


-- reshape your data with ranking in second temp table, as first tem will including all incompleted act
drop table #account_info2 ;

select a.*,
(case when close_date is null then floor(datediff(month,completed_date,GETDATE()))
	else floor(datediff(month,completed_date,close_date)) end) as tenure
 ,rank()over(partition by a.PrimaryClientID order by a.completed_date) as rank_join
into #account_info2
from #account_info a
where PrimaryClientID is not null;





--
select rank_join, count(*) from #account_info2 group by rank_join order by rank_join;
select top 100 * from #account_info2;

select* from #account_info2 where tenure= -59;

-- client 2767 has 224 rank
select * from #account_info2 where PrimaryClientID = '2767' order by completed_date; 


--
select* from #account_info2 where tenure= -59;
select* from [ReportingDW].[act].[Account] where AccountNumber = '51439974';

-- there are 22 account has completed_date > close_date
select* from #account_info2 where completed_date > close_date;





-- look at act
select PrimaryClientID, count(*) as act_num
from #account_info2
where rank_join =1
and PrimaryClientID not in ('170050')
and completed_date > '2011-01-01'

group by PrimaryClientID
order by count(*) desc;

-- client id 170050,125115,449158
select * from #account_info2 where PrimaryClientID = '449158';
select * from [ReportingDW].[act].[Account] where PrimaryClientID = '449158';


-- DataMart.[customer].[uvw_FactBusinessPersonAssetSummary] give you ratio about each acount
-- maringratio
select * from DataMart.[customer].[uvw_FactBusinessPersonAssetSummary]
where BusinessPersonID = 1474
and ProcessingPeriod = 'm'
order by ProcessingDate;







select top 100* from #account_info2;

-- create temp table for client first acct and second acct, 13,819 clients open more than one at same time, outof total 305,057 clinets(4.5%) 
-- only look at open act
drop table #first_act;

select distinct PrimaryClientID, count(distinct AccountClass) as class1_num, min(completed_date) as completed_date_1,min(create_yr) as create_yr_1,
min(tenure) as min_tenure1, max(tenure) as max_tenure1
 into #first_act

from #account_info2
where rank_join =1

group by PrimaryClientID 
;

select AccountClass, count(AccountClass)
from #account_info2 a
where rank_join =1
group by AccountClass;


--5482 client has more than 1 accountclass , 613874
select * from #first_act where class1_num >1 order by class1_num desc;

-- 589374 , 613874
select * from #account_info2 where PrimaryClientID = '613874';
select * from #account_info2 where PrimaryClientID = '589374';

select * from #account_info2 where PrimaryClientID = '568725';
select * from #account_info2 where PrimaryClientID = '665320';


-- 106,557 accts are second acct
drop table #second_act;

select distinct PrimaryClientID, count(PrimaryClientID) as rank2_num, min(completed_date) as completed_date_2,min(create_yr) as create_yr_2,
min(tenure) as min_tenure2, max(tenure) as max_tenure2
 into #second_act

from #account_info2
where rank_join =2
group by PrimaryClientID 
;
-- check the client registered more than 1 act at the first time


floor(datediff(month,a.completed_date1,b.completed_date2)) as td

select a.*, rank2_num,completed_date_2,create_yr_2,min_tenure2,max_tenure2
 into #temp
from #first_act a
left join #second_act b
on a.PrimaryClientID =b.PrimaryClientID

select a.*,floor(datediff(month,a.completed_date_1,a.completed_date_2)) as mth_diff
from #temp a;






-- 
select top 100* from #account_info2;


-- there are 305, 057 client
drop table #client;

select distinct PrimaryClientID, min(completed_date) as join_date, min(create_yr) as join_yr,
(case when floor(datediff(day,min(completed_date),convert(varchar,GETDATE(),23))) >= 365 then 1
	 else 0 end) as one_yr_ind

 into #client
from #account_info2
group by PrimaryClientID;

select top 100* from #client;
-- 494,789 records,,(case when join_date =completed_date then 1 else 0 end) as t0  convert(varchar,GETDATE(),23)
drop table #temp;

select a.*, b.join_date,b.join_yr,
(case when close_date is null then convert(varchar,GETDATE(),23) else close_date end) as time_end
into #temp

from #account_info2 a
inner join #client b
on a.PrimaryClientID = b.PrimaryClientID;

--test
select top 100* from #temp;


-- time snapshot
select  a.*
,(case when join_date =completed_date then 1 else 0 end) as t0
,(case when dateadd(month,1,join_date) between completed_date and time_end then 1 else 0 end) as t1
,(case when dateadd(month,3,join_date) between completed_date and time_end then 1 else 0 end) as t3
,(case when dateadd(month,6,join_date) between completed_date and time_end then 1 else 0 end) as t6
,(case when dateadd(month,12,join_date) between completed_date and time_end then 1 else 0 end) as t12

into #full
from #temp a;


--july, 24th,2019
select top 100* from #full;


--snapshot at t0
--check your nap shot t0 t1 t3 t6 t12
select ind, join_yr,count(PrimaryClientID) as ct

from (select PrimaryClientID, min(create_yr) as join_yr,
	 (case when count(distinct AccountClass) >1 then 'MULTI-CLASS'
			else					min(AccountClass) end) as ind

		from #full
		where t0 = 1 and AccountClass <> 'IN'
		group by PrimaryClientID) b

group by ind,join_yr;






















--your clear version of the code
-- step 1:
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


-- step 2: add rank of each account for the client, and find the tenure for each acct
drop table #account_info2 ;

select a.*,
(case when close_date is null then floor(datediff(month,completed_date,GETDATE()))
	else floor(datediff(month,completed_date,close_date)) end) as tenure
 ,rank()over(partition by a.PrimaryClientID order by a.completed_date) as rank_join
into #account_info2
from #account_info a
where PrimaryClientID is not null;

-- check client 303341 ,330892 give client tenure 0
select* from #account_info2 where PrimaryClientID = '303341';



-- step 3:  at the client level, find there registered time, total client tenure
select top 100* from #client order by MTD;
drop table #client;

select distinct PrimaryClientID, min(completed_date) as join_date, min(create_yr) as join_yr, max(tenure) as client_tenure,
floor(datediff(month,min(completed_date),GETDATE())) as MTD,
(case when floor(datediff(day,min(completed_date),convert(varchar,GETDATE(),23))) >= 365 then 1
	 else 0 end) as one_yr_ind
 into #client
from #account_info2
group by PrimaryClientID;

-- step 4: add end bound of account, to help do the time snapshot
select top 100* from #temp;
drop table #temp;

select a.*, b.join_date,b.join_yr,b.one_yr_ind,b.client_tenure,b.MTD,
(case when close_date is null then convert(varchar,GETDATE(),23) else close_date end) as time_end
into #temp
from #account_info2 a
inner join #client b
on a.PrimaryClientID = b.PrimaryClientID;

-- step 5: time snapshot
select top 100* from #full;
drop table #full;

select  a.*
,(case when join_date =completed_date then 1 else 0 end) as t0
,(case when dateadd(month,1,join_date) between completed_date and time_end then 1 else 0 end) as t1
,(case when dateadd(month,3,join_date) between completed_date and time_end then 1 else 0 end) as t3
,(case when dateadd(month,6,join_date) between completed_date and time_end then 1 else 0 end) as t6
,(case when dateadd(month,12,join_date) between completed_date and time_end then 1 else 0 end) as t12

into #full
from #temp a;

-- step 6:
--snapshot at t0
--check your nap shot t0 t1 t3 t6 t12
select ind, join_yr,count(PrimaryClientID) as ct

from (select PrimaryClientID, min(create_yr) as join_yr,
	 (case when count(distinct AccountClass) >1 then 'MULTI-CLASS'
			else					min(AccountClass) end) as ind

		from #full
		where t0 = 1 and AccountClass <> 'IN'
		group by PrimaryClientID) b

group by ind,join_yr;


select top 100* from  #full;







-----------------------------------------------------------------------------
-- very last step
select distinct a.PrimaryClientID, a.join_yr,a.ind_t0,b.ind_t1,c.ind_t3,d.ind_t6,e.ind_t12,a.client_tenure,a.join_date, a.MTD

	into #class_status

from (select  PrimaryClientID, min(create_yr) as join_yr,min(join_date) as join_date, max(client_tenure) as client_tenure,max(MTD) as MTD,
		(case when count(distinct AccountClass) >1 then 'MULTI-CLASS' else min(AccountClass) end) as ind_t0
		from #full
		where t0 = 1 and AccountClass <> 'IN'
		group by PrimaryClientID) a

left join  (select PrimaryClientID, (case when count(distinct AccountClass) >1 then 'MULTI-CLASS' else min(AccountClass) end) as ind_t1
			from #full
			where t1 = 1 and AccountClass <> 'IN'
			group by PrimaryClientID) b
on a.PrimaryClientID = b.PrimaryClientID

left join  (select PrimaryClientID, (case when count(distinct AccountClass) >1 then 'MULTI-CLASS' else min(AccountClass) end) as ind_t3
			from #full
			where t3 = 1 and AccountClass <> 'IN'
			group by PrimaryClientID) c
on a.PrimaryClientID = c.PrimaryClientID

left join  (select PrimaryClientID, (case when count(distinct AccountClass) >1 then 'MULTI-CLASS' else min(AccountClass) end) as ind_t6
			from #full
			where t6 = 1 and AccountClass <> 'IN'
			group by PrimaryClientID) d
on a.PrimaryClientID = d.PrimaryClientID

left join  (select PrimaryClientID, (case when count(distinct AccountClass) >1 then 'MULTI-CLASS' else min(AccountClass) end) as ind_t12
			from #full
			where t12 = 1 and AccountClass <> 'IN'
			group by PrimaryClientID) e
on a.PrimaryClientID = e.PrimaryClientID
;








-- second process:; deep dive on this
-- 1. acquiration of QWP, are they tranfer from mutual funds?
-- 2. for the client who change class, what's their balance in total change? new worth?   
--     use t0 vs. t12, which is t0 <> t12, and t12 is not null, not look at attrition this time
-- look at client asset at t0, and t12


-- check one person PrimaryClientID='412740', he has WM '60004143', then become multi '51722023' and sd acct
-- this is for movement of asset
 select * from [ReportingDW].[act].[AccountAssetMovement]
 where AccountNumber = '60004143'
order by TransactionDate ;

 select top 100* from [ReportingDW].[act].[AccountEquity_Historical] 
where AccountNumber = '51722023'
order by ProcessingDate ;

select top 100* from [DataMart].[customer].[uvw_FactBusinessPersonAssetSummary];

-- e,m,q,y for processing period
select * from  [DataMart].[customer].[uvw_FactBusinessPersonAssetSummary] 
where BusinessPersonID= '794337'
and ProcessingPeriod = 'm'
order by ProcessingDate;

select top 100* from [ReportingDW].[act].[Account] where PrimaryClientID='412740';

--303,176 crm client id
select count(distinct BusinessPersonID) from [DataMart].[customer].[uvw_FactBusinessPersonAssetSummary];

select top 100* from [ReportingDW].[act].[Client];




--
-- check one person PrimaryClientID='348834', he has WM '30005136', then become multi 26913410 and  sd acct
-- this is for movement of asset
 select * from [ReportingDW].[act].[AccountAssetMovement]
 where AccountNumber = '30005136'
order by TransactionDate ;

 select * from [ReportingDW].[act].[AccountAssetMovement]
 where AccountNumber = '26913410'
order by TransactionDate ;

 select top 100* from [ReportingDW].[act].[AccountEquity_Historical] 
where AccountNumber = '30005136'
order by ProcessingDate ;

select top 100* from [DataMart].[customer].[uvw_FactBusinessPersonAssetSummary];

-- e,m,q,y for processing period
select * from  [DataMart].[customer].[uvw_FactBusinessPersonAssetSummary] 
where BusinessPersonID= '794337'
and ProcessingPeriod = 'y'
order by ProcessingDate;

select top 100* from [ReportingDW].[act].[Account] where PrimaryClientID='348834'
;












-- Aug,14th,2019
-- look at the change status clients, and to see where they transfer more money
-- only look at client join_yr >= 2015 and ind_t0 is sd wm, and ind_t12 is not null

select top 10* from  #class_status;

-- find the client who join after 2015 and have stauts on t0 and t12, and ind_t0 <> ind_t12, only 2879 client left
select *
from #class_status
where join_yr >= 2015
and ind_t0 in ('SD','WM')
and ind_t12 is not null
and ind_t0 <> ind_t12;





-- look at acct with equity 0
select top 100* from [ReportingDW].[act].[AccountEquity_Historical];

select top 100* from [ReportingDW].[act].[AccountSummary];

select top 100* from [DataMart].[qube].[uvw_DimAccount];

