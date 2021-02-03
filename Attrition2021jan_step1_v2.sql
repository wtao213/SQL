-----------------------------------------------------------------------------------
-- start 2021-02-02
-- attrition project

-- it takes 3min27 sec and return 819,456 records
IF OBJECT_ID('tempdb..#account_info') IS NOT NULL
	DROP TABLE #account_info;
select *, rank()over(partition by a.PrimaryClientID order by a.DateCompleted) as rank_join
into #account_info

from 
(select  a.AccountNumber, min(a.DateCompleted) as DateCompleted ,min(convert(varchar,a.CloseDateTime,23)) as close_date
,min(c.STA_Status) as STA_Status
,min(a.AccountStatusID) as  statusID
,min(a.PrimaryClientID) as PrimaryClientID,min(a.PrimaryClientFullName) as PrimaryClientFullName
,min(PrimaryClientEMail) as PrimaryClientEMail
,min(d.BirthDate) as BirthDate
,min(d.Province) as Province,min(d.CityName) as CityName,min(d.StreetName) as StreetName,min(d.StreetNumber) as StreetNumber, min(d.PostalCode) as PostalCode
,min(a.TypeName) as TypeName
,min(b.AccountClass) as AccountClass
,max(a.AccountDetailType) as AccountDetailType-- min(a.IsWealthManaged) as IsWealthManaged,
,min(OfferCode) as OfferCode
,min(a.EquityInCAD) as EquityInCAD 
,min(a.EquityDate) as EquityDate, min(year(DateTimeCompleted)) as act_create_yr
,max(floor(datediff(month,a.DateCompleted,GETDATE()))) as MTD
,max(case when a.CloseDateTime is null then floor(datediff(month,a.DateCompleted,GETDATE())) 
	 else                          floor(datediff(month,a.DateCompleted,convert(varchar,a.CloseDateTime,23))) end) as tenure
,max(d.Income) as Income
,max(d.LiquidAsset) as LiquidAsset
,max(d.NetFixedAsset) as NetFixedAsset
,max(d.NetWorth) as NetWorth
,max(d.Liabilities) as Liabilities
,max(d.MaritalStatus) as MaritalStatus
,max(d.LastName) as LastName
,max(d.FirstName) as FirstName
,max(d.CellPhone) as CellPhone
,max(d.DaytimePhone) as DaytimePhone


from [ReportingDW].[act].[Account] a
inner join (select distinct statusID,STA_Status from [ReportingDWStaging].[crm].[Status]) c
on a.AccountStatusID =c.StatusID
inner join (select AccountNumber,AccountClass,AccountType from [ReportingDW].[client].[Account]) b
on a.AccountNumber =b.AccountNumber
inner join [ReportingDW].[Act].[Client] d
on a.PrimaryClientID = d.ClientID

WHERE isDummy = 0 
AND IsForInternalUse = 0 
AND CommissionPlan NOT LIKE '%Emp%'
AND (a.AccountNumber LIKE '[2-6]%' OR a.AccountNumber LIKE 'Q%')
--AND OrganizationID IS NULL
AND IsHouse = 0 
AND IsOffset = 0 
AND DateTimeCompleted IS NOT NULL
AND TypeName <> 'FOREX' 
group by a.AccountNumber) a
;






-------------------------------------------------------------------------------
-- active acct each month
-- it take 16 sec with 49 rows
IF OBJECT_ID('tempdb..#test') IS NOT NULL
BEGIN
   	DROP TABLE #test;
END

CREATE TABLE #test (
    [yr] INT,
    [mth] INT,
    [act_ct] INT,
	[client_ct] INT
);

DECLARE @StartDT DATETIME
SET @StartDT =  '20170101'

BEGIN TRY
WHILE @StartDT < '20210201'
BEGIN
    INSERT INTO #test (
    [yr] 
   ,[mth] 
   ,[act_ct] 
   ,[client_ct]
	)
	SELECT year(@StartDT)as yr, month(@StartDT) as mth,COUNT(*) as act_ct,COUNT(distinct PrimaryClientID) as client_ct
	 FROM #account_info
	 WHERE DateCompleted <= @StartDT
	 AND (close_date is null or close_date >= DATEADD(MONTH,1,@StartDT))

	SET @StartDT = DATEADD(MONTH,1,@StartDT)

END
END TRY

BEGIN CATCH
    IF OBJECT_ID('tempdb..#test') IS NOT NULL
    BEGIN
        DROP TABLE #test
    END
END CATCH

select* from #test;






--------------------------------------------------------------------------------------
-- part 2: look at 1. total attried(no active acct 14days after window)
--							1.1 new client(<=3 months) attried
--				   2. new client closing
--				   3. remaining
-- 2021-02-03 add in acct info as well, today 29 sec
IF OBJECT_ID('tempdb..#test') IS NOT NULL
BEGIN
   	DROP TABLE #test;
END

CREATE TABLE #test (
	[yr] INT
   ,[mth] INT
   ,[attried_client] INT
   ,[attried_new_client] INT
   ,[closing_new_client] INT
   ,[closing_other_client] INT
   ,[swap_new_client] INT
   ,[swap_other_client] INT
   ,[attried_act] INT
   ,[attried_new_act] INT
   ,[closing_new_act] INT
   ,[closing_other_act] INT
   ,[swap_new_act] INT
   ,[swap_other_act] INT
);

DECLARE @StartDT DATETIME
SET @StartDT =  '20170101'

BEGIN TRY
WHILE @StartDT < '20210201'
BEGIN
    INSERT INTO #test (
	[yr] 
   ,[mth] 
   ,[attried_client] 
   ,[attried_new_client]
   ,[closing_new_client]
   ,[closing_other_client]
   ,[swap_new_client]
   ,[swap_other_client]
   ,[attried_act]
   ,[attried_new_act]
   ,[closing_new_act]
   ,[closing_other_act]
   ,[swap_new_act]
   ,[swap_other_act]
	)
	 select year(@StartDT)as yr, month(@StartDT) as mth
	,sum(case when act_act = 0				      then 1 else 0 end) as attried_client
	,sum(case when act_act = 0 and tenure_we <=3  then 1 else 0 end) as attried_new_client
	,sum(case when act_act <>0 and tenure_we <=3  then 1 else 0 end) as closing_new_client
	,sum(case when act_act <>0 and tenure_we >3   then 1 else 0 end) as closing_other_client 
	,sum(case when act_act <>0 and tenure_we <=3 and (max_opendate between DATEADD(day,-14,@StartDT) and DATEADD(day,44,@StartDT)) then 1 else 0 end) as swap_new_client
	,sum(case when act_act <>0 and tenure_we >3  and (max_opendate between DATEADD(day,-14,@StartDT) and DATEADD(day,44,@StartDT)) then 1 else 0 end) as swap_other_client

	,sum(case when act_act = 0				      then closed_act else 0 end) as attried_act
	,sum(case when act_act = 0 and tenure_we <=3  then closed_act else 0 end) as attried_new_act
	,sum(case when act_act <>0 and tenure_we <=3  then closed_act else 0 end) as closing_new_act
	,sum(case when act_act <>0 and tenure_we >3   then closed_act else 0 end) as closing_other_act			 
	,sum(case when act_act <>0 and tenure_we <=3 and (max_opendate between DATEADD(day,-14,@StartDT) and DATEADD(day,44,@StartDT)) then closed_act else 0 end) as swap_new_act
	,sum(case when act_act <>0 and tenure_we >3  and (max_opendate between DATEADD(day,-14,@StartDT) and DATEADD(day,44,@StartDT)) then closed_act else 0 end) as swap_other_act



	from (select a.PrimaryClientID
			,max(closed_act) as closed_act
			,(case when (count(*) - count(close_date)) = 0 then floor(datediff(month,min(DateCompleted),max(close_date))) 
				   else floor(datediff(month,min(DateCompleted),DATEADD(MONTH,1,@StartDT))) end) as tenure_we
			,sum(case when DateCompleted < DATEADD(day,44,@StartDT) and (close_date is null or close_date > DATEADD(day,44,@StartDT)) then 1 else 0 end) as act_act -- 14 days after window close still no active acct
			,max(a.max_close_date) as max_close_date
			,max(case when (close_date is null or close_date > DATEADD(MONTH,1,@StartDT) and DateCompleted < DATEADD(MONTH,1,@StartDT)) then DateCompleted else null end) as max_opendate

			from (select PrimaryClientID,max(close_date) as max_close_date,count(distinct AccountNumber) as closed_act
				  from #account_info where close_date >= @StartDT  and close_date < DATEADD(MONTH,1,@StartDT) group by PrimaryClientID) a

			inner join (select* from #account_info where DateCompleted < DATEADD(day,44,@StartDT)) b -- only look at the acct completed before window end+14 days
			on a.PrimaryClientID = b.PrimaryClientID
			group by a.PrimaryClientID) c

	SET @StartDT = DATEADD(MONTH,1,@StartDT)

END
END TRY

BEGIN CATCH
    IF OBJECT_ID('tempdb..#test') IS NOT NULL
    BEGIN
        DROP TABLE #test
    END
END CATCH


-- look at the results
select* from #test;