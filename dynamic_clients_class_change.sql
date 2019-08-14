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
