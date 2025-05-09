USE [Reports]
GO
/****** Object:  StoredProcedure [dbo].[FI_15_03_CUSTOMERCASH_BPSA2]    Script Date: 4/9/2024 2:04:28 PM ******/
SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

/*

exec [dbo].[FI_15_03_CUSTOMERCASH_BPSA2] 'LUCO','All' , '2024-03-19', 'batch'
exec [dbo].[FI_15_03_CUSTOMERCASH_BPSA] 'CMBK','All' , '2024-03-08','batch'


exec [dbo].[FI_15_03_CUSTOMERCASH_BPSA2] 'All','All' , '2024-03-14','batch'


*/
--sp_recompile '[dbo].[FI_15_03_CUSTOMERCASH_TSS]'
--exec [dbo].[FI_15_03_CUSTOMERCASH_BPSA2] 'All','All' , '2024-03-14','batch'


ALTER proc [dbo].[FI_15_03_CUSTOMERCASH_BPSA2]
 (
 --DECLARE
@Correspondent   nvarchar(100) = 'All' ,
@Trader nvarchar(100) = 'ALL' ,
@systemDate Date --='03/14/2024'
,@DataMode   nvarchar(20)= 'Batch'  --='2021-04-22'

, @username nvarchar (75) = 'rpt_user'
) As begin 

 SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED
Drop table if exists #temp ,#temp_fail ,#realtime , #activity ,#open ,#close ,#balances
DROP TABLE IF EXISTS #final1,#balance1
DROP TABLE IF EXISTS #final2,#balance2 ,#output ,#temp2


DECLARE @Settledate VArchar(10) , @outstandingdate VArchar(10)
Select @Settledate = CONVERT(VArchar(10) ,Staging.[admin].[Get_NextGoodBusinessDay](  @Systemdate),112) 
Select @outstandingdate = CONVERT(VArchar(10) ,(  @Systemdate),112) 



/**********/
   DECLARE @S_Date DATE   ,@S_dateformat varchar(12)
		
		SET @S_Date=  @systemDate 
		Set @S_dateformat = (SElect CONVERT(VArchar(12) ,@S_Date,112) )


/**********/

DECLARE @mockdate date
Set @mockdate = (Select weekenddate from dw.admin.Mockdate)

--select @Settledate , @systemDate

Select DISTINCT
LTRIM(RTRIM(UPPER(LTRIM(RTRIM(e.DFIRM ))) )) AS	[Firmname]
,LTRIM(RTRIM(e.Enitty_code )) AS	[entitycode]
,LTRIM(RTRIM('' )) AS	[trader]
,LTRIM(RTRIM('' )) AS	[ACCOUNT NUMBER]
,LTRIM(RTRIM('' )) AS	[impact trans num] ,LTRIM(RTRIM( '')) [EXT REF]
,LTRIM(RTRIM( '')) AS	[SETT DT]
,CONVERT(BIGINT,null )  AS	[Long/Short quantity]
,LTRIM(RTRIM('OPENING BALANCE' )) AS	[DESCRIPTION]
,LTRIM(RTRIM('' )) AS	[CPN]
,LTRIM(RTRIM('' )) AS	[MATURITY]
,LTRIM(RTRIM('' )) AS	[REC/DEL]
,LTRIM(RTRIM('' )) AS	[B/S]
,LTRIM(RTRIM('' )) AS	[CONTRA]
,SUM(CONVERT(MONEY,ISNULL(a.ydys_settlm_dt_amt,0)))  AS	[Debit/Credit amount]
,LTRIM(RTRIM(DW.mapping.get_all_currency_cd(a.currency_cd)  )) AS	[CCY]
,CONVERT(nvarchar(100), LTRIM(RTRIM('' )) ) AS	[CUSTOMER ADDRESS 1]
,CONVERT(nvarchar(100), LTRIM(RTRIM('' )) ) AS	  
,CONVERT(nvarchar(100), LTRIM(RTRIM('' )) ) AS	[CUSTOMER ADDRESS 3]
,CONVERT(nvarchar(100), LTRIM(RTRIM('' )) ) AS	[CUSTOMER ADDRESS 4]
,CONVERT(nvarchar(100), LTRIM(RTRIM('' )) ) AS	[CUSTOMER ADDRESS 5]
,LTRIM(RTRIM('OPEN' )) AS	[TYPE]
,CONVERT(DECIMAL(16,4) ,0 ) AS	[MKT Price]
,CONVERT(MONEY, 0 ) AS	[MKT Value]
,10  AS	[orderby]
,LTRIM(RTRIM('' )) AS	[Cusip]
,LTRIM(RTRIM('' )) AS	[ISIN]

, ''[Contra Account Number] 
 ,'' [Name]

,1 as cnt 
  into #open
  --select *
FROM  DW.derived.Entity_Firm_Names e 
		LEFT JOIn Dw.data.View_TACC_TYPE_BALANCE A on e.Branch_Cd =  a.branch_cd
			AND CONVERT(DATE, systemdate, 101) = CONVERT(DATE,  Staging.admin.Get_PrevGoodBusinessDay(@systemDate), 101)
			    And ( a.account_cd like '90%' or a.account_cd like '91%')
				--and type_account_cd =1
			  -- AND h#glac = '011200101X' 
			  and  e.Enitty_code <> 'iCBC' 
			 
WHERE e.Enitty_code <> ''
        AND Ltrim(Rtrim(ISNULL(e.Enitty_code,@Correspondent) )) = CASE 
											WHEN @Correspondent = 'All' THEN Ltrim(Rtrim(ISNULL(e.Enitty_code,@Correspondent) ))
											ELSE @Correspondent 
										  END 
GROUP by LTRIM(RTRIM(UPPER(LTRIM(RTRIM(e.DFIRM ))) )) --AS	[Firmname]
,LTRIM(RTRIM(e.Enitty_code )) --AS	[entitycode]
,LTRIM(RTRIM(DW.mapping.get_all_currency_cd(a.currency_cd)  )) --	[CCY]



Select DISTINCT
LTRIM(RTRIM(UPPER(LTRIM(RTRIM(e.DFIRM ))) )) AS	[Firmname]
,LTRIM(RTRIM(e.Enitty_code )) AS	[entitycode]
,LTRIM(RTRIM('' )) AS	[trader]
,LTRIM(RTRIM('' )) AS	[ACCOUNT NUMBER]
,LTRIM(RTRIM('' )) AS	[impact trans num] ,LTRIM(RTRIM( '')) [EXT REF]
,LTRIM(RTRIM( '')) AS	[SETT DT]
,CONVERT(BIGINT,null )  AS	[Long/Short quantity]
,LTRIM(RTRIM('CLOSING BALANCE' )) AS	[DESCRIPTION]
,LTRIM(RTRIM('' )) AS	[CPN]
,LTRIM(RTRIM('' )) AS	[MATURITY]
,LTRIM(RTRIM('' )) AS	[REC/DEL]
,LTRIM(RTRIM('' )) AS	[B/S]
,LTRIM(RTRIM('' )) AS	[CONTRA]
,SUM(CONVERT(MONEY,ISNULL(a.ydys_settlm_dt_amt,0)))  AS	[Debit/Credit amount]
,LTRIM(RTRIM(DW.mapping.get_all_currency_cd(a.currency_cd)  )) AS	[CCY]
,CONVERT(nvarchar(100), LTRIM(RTRIM('' )) ) AS	[CUSTOMER ADDRESS 1]
,CONVERT(nvarchar(100), LTRIM(RTRIM('' )) ) AS	[CUSTOMER ADDRESS 2]
,CONVERT(nvarchar(100), LTRIM(RTRIM('' )) ) AS	[CUSTOMER ADDRESS 3]
,CONVERT(nvarchar(100), LTRIM(RTRIM('' )) ) AS	[CUSTOMER ADDRESS 4]
,CONVERT(nvarchar(100), LTRIM(RTRIM('' )) ) AS	[CUSTOMER ADDRESS 5]
,LTRIM(RTRIM('CLOSE' )) AS	[TYPE]
,CONVERT(DECIMAL(16,4) ,0 ) AS	[MKT Price]
,CONVERT(MONEY, 0 ) AS	[MKT Value]
,90  AS	[orderby]
,LTRIM(RTRIM('' )) AS	[Cusip]
,LTRIM(RTRIM('' )) AS	[ISIN]

, ''[Contra Account Number] 
 ,'' [Name]
----select *
,1 as cnt 
  into #close
FROM  DW.derived.Entity_Firm_Names e 
		LEFT JOIn Dw.data.View_TACC_TYPE_BALANCE A on e.Branch_Cd =  a.branch_cd
			AND CONVERT(DATE, systemdate, 101) = CONVERT(DATE,  @systemDate, 101) 
			      And ( a.account_cd like '90%' or a.account_cd like '91%')
				--and type_account_cd =1
			  -- AND h#glac = '011200101X' 
			  and  e.Enitty_code <> 'iCBC' 
			 
WHERE e.Enitty_code <> ''
        AND Ltrim(Rtrim(ISNULL(e.Enitty_code,@Correspondent) )) = CASE 
											WHEN @Correspondent = 'All' THEN Ltrim(Rtrim(ISNULL(e.Enitty_code,@Correspondent) ))
											ELSE @Correspondent 
										  END 
								
 GROUP by LTRIM(RTRIM(UPPER(LTRIM(RTRIM(e.DFIRM ))) )) --AS	[Firmname]
,LTRIM(RTRIM(e.Enitty_code )) --AS	[entitycode]
,LTRIM(RTRIM(DW.mapping.get_all_currency_cd(a.currency_cd)  )) --	[CCY]

      


SELECT DISTINCT
ISNULL(a.[Firmname],b.[Firmname]) [Firmname],
ISNULL(a.[entitycode],b.[entitycode]) [entitycode],
ISNULL(a.[trader],b.[trader]) [trader],
ISNULL(a.[ACCOUNT NUMBER],b.[ACCOUNT NUMBER]) [ACCOUNT NUMBER],
ISNULL(a.[impact trans num],b.[impact trans num]) [impact trans num],
ISNULL(a.[EXT REF],b.[EXT REF]) [EXT REF],
ISNULL(a.[SETT DT],b.[SETT DT]) [SETT DT],
ISNULL(a.[Long/Short quantity],b.[Long/Short quantity]) [Long/Short quantity],
LTRIM(RTRIM('OPENING BALANCE' )) [DESCRIPTION],
ISNULL(a.[CPN],b.[CPN]) [CPN],
ISNULL(a.[MATURITY],b.[MATURITY]) [MATURITY],
ISNULL(a.[REC/DEL],b.[REC/DEL]) [REC/DEL],
ISNULL(a.[B/S],b.[B/S]) [B/S],
ISNULL(a.[CONTRA],b.[CONTRA]) [CONTRA],
ISNULL(a.[Debit/Credit amount],0) [Debit/Credit amount],
ISNULL(a.CCY,b.CCY) AS	[CCY],

ISNULL(a.[CUSTOMER ADDRESS 1],b.[CUSTOMER ADDRESS 1]) [CUSTOMER ADDRESS 1],
ISNULL(a.[CUSTOMER ADDRESS 2],b.[CUSTOMER ADDRESS 1]) [CUSTOMER ADDRESS 2],
ISNULL(a.[CUSTOMER ADDRESS 3],b.[CUSTOMER ADDRESS 3]) [CUSTOMER ADDRESS 3],
ISNULL(a.[CUSTOMER ADDRESS 4],b.[CUSTOMER ADDRESS 4]) [CUSTOMER ADDRESS 4],
ISNULL(a.[CUSTOMER ADDRESS 5],b.[CUSTOMER ADDRESS 5]) [CUSTOMER ADDRESS 5],
LTRIM(RTRIM('OPEN' )) [TYPE],
ISNULL(a.[MKT Price],0) [MKT Price],
ISNULL(a.[MKT Value],0) [MKT Value],
10 [orderby],
ISNULL(a.[Cusip],b.[Cusip]) [Cusip],
ISNULL(a.[ISIN],b.[ISIN]) [ISIN],
ISNULL(a.[Contra Account Number],b.[Contra Account Number]) [Contra Account Number],
ISNULL(a.[Name],b.[Name]) [Name],
ISNULL(a.[cnt],b.[cnt]) [cnt]


into #balances
From #open a
Full outer join #close b  on a.entitycode =b.entitycode 


union 

SELECT DISTINCT
ISNULL(a.[Firmname],b.[Firmname]) [Firmname],
ISNULL(a.[entitycode],b.[entitycode]) [entitycode],
ISNULL(a.[trader],b.[trader]) [trader],
ISNULL(a.[ACCOUNT NUMBER],b.[ACCOUNT NUMBER]) [ACCOUNT NUMBER],
ISNULL(a.[impact trans num],b.[impact trans num]) [impact trans num],
ISNULL(a.[EXT REF],b.[EXT REF]) [EXT REF],
ISNULL(a.[SETT DT],b.[SETT DT]) [SETT DT],
ISNULL(a.[Long/Short quantity],b.[Long/Short quantity]) [Long/Short quantity],
LTRIM(RTRIM('CLOSING BALANCE' )) [DESCRIPTION],
ISNULL(a.[CPN],b.[CPN]) [CPN],
ISNULL(a.[MATURITY],b.[MATURITY]) [MATURITY],
ISNULL(a.[REC/DEL],b.[REC/DEL]) [REC/DEL],
ISNULL(a.[B/S],b.[B/S]) [B/S],
ISNULL(a.[CONTRA],b.[CONTRA]) [CONTRA],
ISNULL(a.[Debit/Credit amount],0) [Debit/Credit amount],
ISNULL(a.CCY,b.CCY) AS	[CCY],
ISNULL(a.[CUSTOMER ADDRESS 1],b.[CUSTOMER ADDRESS 1]) [CUSTOMER ADDRESS 1],
ISNULL(a.[CUSTOMER ADDRESS 2],b.[CUSTOMER ADDRESS 1]) [CUSTOMER ADDRESS 2],
ISNULL(a.[CUSTOMER ADDRESS 3],b.[CUSTOMER ADDRESS 3]) [CUSTOMER ADDRESS 3],
ISNULL(a.[CUSTOMER ADDRESS 4],b.[CUSTOMER ADDRESS 4]) [CUSTOMER ADDRESS 4],
ISNULL(a.[CUSTOMER ADDRESS 5],b.[CUSTOMER ADDRESS 5]) [CUSTOMER ADDRESS 5],
LTRIM(RTRIM('CLOSE' )) [TYPE],
ISNULL(a.[MKT Price],0) [MKT Price],
ISNULL(a.[MKT Value],0) [MKT Value],
90 [orderby],
ISNULL(a.[Cusip],b.[Cusip]) [Cusip],
ISNULL(a.[ISIN],b.[ISIN]) [ISIN],
ISNULL(a.[Contra Account Number],b.[Contra Account Number]) [Contra Account Number],
ISNULL(a.[Name],b.[Name]) [Name],
ISNULL(a.[cnt],b.[cnt]) [cnt]

From #close a
Full outer join #open b  on a.entitycode =b.entitycode



/***********************NEW ACTIVITY *****************/
truncate table Reports.daily_reports.get_ListTrans



  insert into Reports.daily_reports.get_ListTrans
  exec [ICBCFSAPPDB].screen.[get_ListTrans] 
  --@SystemStartDate = @systemdate
   @SystemEndDate   =@systemdate
   ,@SettleEndDate   =@systemdate  
  -- ,@SettleStartDate   =@systemdate 
  ,@UserName ='rpt_user'
  ,  @Branch_CD='All', @Account_CD_Min='90000',      @Account_CD_Max='92000'
  --,@AcctType ='1'
  --,@Currency_cd ='usd' 
  ,@Cancelled ='all excluding cancelled'




delete a FROM Reports.daily_reports.get_ListTrans a
  Where settledate <> @systemdate


Delete a from Reports.daily_reports.get_ListTrans a
where branch_cd < 100

Update a set a.Description = b.Description
--Select a.Description,b.Description ,a.* 
from Reports.daily_reports.get_ListTrans a
LEFT join DW.data.View_TDIVIDEND_TRANS b on a.[Acct No] = b.branch_cd +b.account_cd 
			and a.SettleDate = b.transaction_dt
			and a.BRSecID = b.security_adp_nbr
--Where Cusip ='46647PDD5'
Where a.Description = ''




SELECT   Convert (nvarchar (100),e.DFIRM,101) as Firmname,
		  e.Enitty_code entitycode, 
       ''  trader, a.[acct no] AS [ACCOUNT NUMBER],
      CONVERT(nvarchar(250), A.[TradeID])  [impact trans num], A.[TradeID] [EXT REF],
       FORMAT(CONVERT (DATE, a.[Settledate], 101),'MM/dd/yyyy') AS 'SETT DT', 
	Convert(decimal(36,6) , a.qty )   [Long/Short quantity],
       

       CONVERT(varchar(250), a.Description  )                AS 'DESCRIPTION', 
       bkcsp                       AS 'CPN', 
      SUBSTRING(bkmdt,5,2)     +'/' + SUBSTRING(bkmdt,7,2) +'/' +   SUBSTRING(bkmdt,1,4)                   AS 'MATURITY', 
     CONVERT(varchar(20),  debit_credit_cd   )                   AS 'REC/DEL', 
	  A.EntryType
				AS [B/S],       ---NEW VN 09/27/22
       cu.CUCNIK                      AS 'CONTRA', 
      CONVERT(DECIMAL(36,6) , CAse WHEN debit_credit_cd ='C' and srltbl ='TDIVIDEND_TRANS'
					 Then -1 * A.[Net amt] else  A.[Net amt] end  )   as [Debit/Credit amount],  ---VN no convertion -- need to show local currency values
	   a.currency AS	[CCY],
	  
	   0    AS 'OPENNING BAL'
	   
	   , Convert (nvarchar (100),'',101) as [CUSTOMER ADDRESS 1],
	   Convert (nvarchar (100),'',101) as [CUSTOMER ADDRESS 2],
	   Convert (nvarchar (100),'',101) as [CUSTOMER ADDRESS 3],
	   Convert (nvarchar (100),'',101) as [CUSTOMER ADDRESS 4],
	   Convert (nvarchar (100),'',101) as [CUSTOMER ADDRESS 5],

	  CONVERT(VARCHAR(250) , 'ACTION' ) as [TYPE], 
	   
	   Convert (decimal (20,9),0,101) as [MKT Price], CONVERT(DECIMAL(36,6) ,0,101) as [MKT Value],
       30                          AS orderby ,a.Cusip,A.ISIN, Convert (nvarchar (100),CUCACT,101) as [Contra Account Number], Convert (nvarchar (100),'',101) as [Name]
	  
	   into #temp 
	   --select *
FROM   Reports.daily_reports.get_ListTrans A

  Inner JOIN dw.Derived.Entity_Firm_Names e   on LTRIM(RTRIM(a.Branch_Cd ))  =  LTRIM(RTRIM( e.Branch_Cd))
  left join [DW].data.View_BECFILEP B on A.Cusip = B.BCUSP --and CONVERT(DATE, a.systemDate, 101) = CONVERT(DATE, b.systemDate, 101)
    inner join [DW].data.View_cusfilep cu on  a.[acct no] = cu.CUCACT and cu.CUENCD ='ICBC'

   left join [DW].data.View_TCURRENCY_CVRSN C on a.Currency = iso_crncy_cd
	and CONVERT(DATE, @systemDate, 101) = CONVERT(DATE, c.systemDate, 101)


WHERE 1=1                                 
		and e.Enitty_code <> 'iCBC'

          AND Ltrim(Rtrim(e.Enitty_code)) = CASE 
                                    WHEN @Correspondent = 'All' THEN Ltrim(Rtrim(e.Enitty_code)) 
                                    ELSE @Correspondent 
                                  END 


update b set b.FirmName= UPPER(LTRIM(RTRIM(e.DFIRM )))
			from 	   #temp  b
			Inner Join dw.Derived.Entity_Firm_Names e on e.Enitty_code = b.EntityCode Collate SQL_Latin1_General_CP1_CI_AS
			



Update  a 
set 

[CUSTOMER ADDRESS 1]=   b.[CUSTOMER ADDRESS 1]
,[CUSTOMER ADDRESS 2] =  b.[CUSTOMER ADDRESS 2] 
,[CUSTOMER ADDRESS 3] = b.[CUSTOMER ADDRESS 3] 
,[CUSTOMER ADDRESS 4] = b.[CUSTOMER ADDRESS 4]
,[CUSTOMER ADDRESS 5] = b.[CUSTOMER ADDRESS 5] 
 --select distinct Firmname, b.*
from  #temp  a  join    dbo.fn_Get_FirmName_Address('All') b on 
 LTRIM(RTRIM(a.EntityCode))= LTRIM(RTRIM(b.Entity_code)) --or LEFT(a.firmname,12) = LEFT(CUCSNA,12)
 Where 1=1 

	
  Update a 
  set 
  a.[MKT Value] =  
				(ISNULL([Long/Short quantity],0))	* ( [dbo].[FORMATTED_PRICE] (b.[CALCULABLE CLOSING PRICE - ASK] )) 
  ,[Mkt Price] =( [dbo].[FORMATTED_PRICE] (b.[CALCULABLE CLOSING PRICE - ASK] )) 

  --select *
  from #temp a 
    INNER JOIN dw.Derived.becfilep AS b 
   ON a.Cusip = b.BCUSP   where SystemDate=@systemDate




Select 
LTRIM(RTRIM(Firmname )) AS	[Firmname]
,LTRIM(RTRIM(entitycode )) AS	[entitycode]
,LTRIM(RTRIM(trader )) AS	[trader]
,LTRIM(RTRIM([ACCOUNT NUMBER] )) AS	[ACCOUNT NUMBER]
,LTRIM(RTRIM([impact trans num] )) AS	[impact trans num] ,LTRIM(RTRIM( [EXT REF])) [EXT REF]
,LTRIM(RTRIM( [SETT DT])) AS	[SETT DT]
,CONVERT(BIGINT,[Long/Short quantity] )  AS	[Long/Short quantity]
 
,LTRIM(RTRIM([DESCRIPTION] )) AS	[DESCRIPTION]
,LTRIM(RTRIM(CPN )) AS	[CPN]
,LTRIM(RTRIM(MATURITY )) AS	[MATURITY]
,LTRIM(RTRIM([REC/DEL] )) AS	[REC/DEL]
,LTRIM(RTRIM([B/S] )) AS	[B/S]
,LTRIM(RTRIM(CONTRA )) AS	[CONTRA]
,CONVERT(MONEY,[Debit/Credit amount] ) AS	[Debit/Credit amount]
, case when CONVERT(MONEY,[Debit/Credit amount] ) >= 0 then 'D' else'C' end rdcd
,LTRIM(RTRIM(CCY )) AS	[CCY] 
,LTRIM(RTRIM([CUSTOMER ADDRESS 1] )) AS	[CUSTOMER ADDRESS 1]
,LTRIM(RTRIM([CUSTOMER ADDRESS 2] )) AS	[CUSTOMER ADDRESS 2]
,LTRIM(RTRIM([CUSTOMER ADDRESS 3] )) AS	[CUSTOMER ADDRESS 3]
,LTRIM(RTRIM([CUSTOMER ADDRESS 4] )) AS	[CUSTOMER ADDRESS 4]
,LTRIM(RTRIM([CUSTOMER ADDRESS 5] )) AS	[CUSTOMER ADDRESS 5]
,LTRIM(RTRIM(TYPE )) AS	[TYPE]
,CONVERT(DECIMAL(16,4) ,[MKT Price] ) AS	[MKT Price]
,CONVERT(MONEY, [MKT Value] ) AS	[MKT Value]
,orderby  AS	[orderby]
,LTRIM(RTRIM(Cusip )) AS	[Cusip]
,LTRIM(RTRIM(ISIN )) AS	[ISIN]

, [Contra Account Number] 
 ,[Name]
----select *
,1 as cnt into #activity
from #balances

UNION ALL

Select 
LTRIM(RTRIM(Firmname )) AS	[Firmname]
,LTRIM(RTRIM(entitycode )) AS	[entitycode]
,LTRIM(RTRIM(trader )) AS	[trader]
,LTRIM(RTRIM([ACCOUNT NUMBER] )) AS	[ACCOUNT NUMBER]
,LTRIM(RTRIM([impact trans num] )) AS	[impact trans num] ,LTRIM(RTRIM( [EXT REF])) [EXT REF]
,LTRIM(RTRIM( FORMAT(CONVERT (DATE, [SETT DT], 101),'MM/dd/yyyy') )) AS	[SETT DT]
,CONVERT(BIGINT,[Long/Short quantity] )  AS	[Long/Short quantity]
 
,LTRIM(RTRIM([DESCRIPTION] )) AS	[DESCRIPTION]
,LTRIM(RTRIM(CPN )) AS	[CPN]
,LTRIM(RTRIM(  MATURITY  )) AS	[MATURITY]
,LTRIM(RTRIM([REC/DEL] )) AS	[REC/DEL]
,LTRIM(RTRIM([B/S] )) AS	[B/S]
,LTRIM(RTRIM(CONTRA )) AS	[CONTRA]
,CONVERT(MONEY,[Debit/Credit amount] ) AS	[Debit/Credit amount]
, case when CONVERT(MONEY,[Debit/Credit amount] ) >= 0 then 'D' else'C' end rdcd
,LTRIM(RTRIM(CCY )) AS	[CCY] 
,LTRIM(RTRIM([CUSTOMER ADDRESS 1] )) AS	[CUSTOMER ADDRESS 1]
,LTRIM(RTRIM([CUSTOMER ADDRESS 2] )) AS	[CUSTOMER ADDRESS 2]
,LTRIM(RTRIM([CUSTOMER ADDRESS 3] )) AS	[CUSTOMER ADDRESS 3]
,LTRIM(RTRIM([CUSTOMER ADDRESS 4] )) AS	[CUSTOMER ADDRESS 4]
,LTRIM(RTRIM([CUSTOMER ADDRESS 5] )) AS	[CUSTOMER ADDRESS 5]
,LTRIM(RTRIM(TYPE )) AS	[TYPE]
,CONVERT(DECIMAL(16,4) ,[MKT Price] ) AS	[MKT Price]
,CONVERT(MONEY, [MKT Value] ) AS	[MKT Value]
,orderby  AS	[orderby]
,LTRIM(RTRIM(Cusip )) AS	[Cusip]
,LTRIM(RTRIM(ISIN )) AS	[ISIN]

, [Contra Account Number] 
 ,[Name]
----select *
,1 as cnt 
from #temp

--UNION ALL

--Select DISTINCT
--LTRIM(RTRIM(Firmname )) AS	[Firmname]
--,LTRIM(RTRIM([MAP TO CORRESPONDENT] )) AS	[entitycode]
--,LTRIM(RTRIM('' )) AS	[trader]
--,LTRIM(RTRIM(account_cd )) AS	[ACCOUNT NUMBER]
--,LTRIM(RTRIM(trans )) AS	[impact trans num] ,LTRIM(RTRIM( '')) [EXT REF]
--,LTRIM(RTRIM( null)) AS	[SETT DT]
--,CONVERT(INT,0 )  AS	[Long/Short quantity]
 
--,LTRIM(RTRIM(TYPE )) AS	[DESCRIPTION]
--,LTRIM(RTRIM(0 )) AS	[CPN]
--,LTRIM(RTRIM(null )) AS	[MATURITY]
--,LTRIM(RTRIM('' )) AS	[REC/DEL]
--,LTRIM(RTRIM('' )) AS	[B/S]
--,LTRIM(RTRIM('' )) AS	[CONTRA]
--,CONVERT(MONEY,[Debit/Credit amount] ) AS	[Debit/Credit amount]
--, case when CONVERT(MONEY,[Debit/Credit amount] ) >= 0 then 'D' else'C' end rdcd
--,LTRIM(RTRIM(CCY )) AS	[CCY] 
--,LTRIM(RTRIM('')) AS	[CUSTOMER ADDRESS 1]
--,LTRIM(RTRIM('')) AS	[CUSTOMER ADDRESS 2]
--,LTRIM(RTRIM('')) AS	[CUSTOMER ADDRESS 3]
--,LTRIM(RTRIM('')) AS	[CUSTOMER ADDRESS 4]
--,LTRIM(RTRIM('')) AS	[CUSTOMER ADDRESS 5]
--,LTRIM(RTRIM('CASH' )) AS	[TYPE]
--,CONVERT(DECIMAL(16,4) ,0 ) AS	[MKT Price]
--,CONVERT(MONEY, 0 ) AS	[MKT Value]
--,40  AS	[orderby]
--,LTRIM(RTRIM('' )) AS	[Cusip]
--,LTRIM(RTRIM('' )) AS	[ISIN]

--,'' as  [Contra Account Number] 
-- ,'' as [Name]
 
--,1 as cnt 
--from #realtime
----Where [MAP TO CORRESPONDENT] in (Select Distinct EntityCode  from #temp)


Update  a 
set 

[CUSTOMER ADDRESS 1]= b.[CUSTOMER ADDRESS 1]
,[CUSTOMER ADDRESS 2] =  b.[CUSTOMER ADDRESS 2] 
,[CUSTOMER ADDRESS 3] = b.[CUSTOMER ADDRESS 3] 
,[CUSTOMER ADDRESS 4] = b.[CUSTOMER ADDRESS 4]
,[CUSTOMER ADDRESS 5] = b.[CUSTOMER ADDRESS 5] 
 --select distinct Firmname, b.*
from  #activity  a  join   reports.dbo.fn_Get_FirmName_Address('All')  b on 
 LTRIM(RTRIM(a.Firmname))= LTRIM(RTRIM(b.Entity_code)) --or LEFT(a.firmname,12) = LEFT(CUCSNA,12)
 Where 1=1 




DROP TABLE IF EXISTS #final2,#balance2

--declare @username varchar(50) ='rpt_user'
Select * into #final2 
from #activity
Where ISNULL(DESCRIPTION,'') <> 'CLOSING BALANCE'
And exists  (select * from  ICBCFSAPPDB. [dbo].[Get_User_AccountAccesscontrol]  (@UserName)  C  where  entitycode =case when  C.Branch='ALL'  then entitycode else c.Branch  end 
                      and        trader=   case     when    c.AccountNo='ALL'   then    trader  else C.AccountNo  end  )

UNION ALL

Select 
LTRIM(RTRIM(Firmname )) AS	[Firmname]
,LTRIM(RTRIM(entitycode )) AS	[entitycode]
,LTRIM(RTRIM('' )) AS	[trader]
,LTRIM(RTRIM('' )) AS	[ACCOUNT NUMBER]
,LTRIM(RTRIM('' )) AS	[impact trans num] ,LTRIM(RTRIM( '')) [EXT REF]
,LTRIM(RTRIM( '')) AS	[SETT DT]
,CONVERT(INT,null )  AS	[Long/Short quantity]
 
,LTRIM(RTRIM('CLOSING BALANCE' )) AS	[DESCRIPTION]
,LTRIM(RTRIM('' )) AS	[CPN]
,LTRIM(RTRIM('' )) AS	[MATURITY]
,LTRIM(RTRIM('' )) AS	[REC/DEL]
,LTRIM(RTRIM('' )) AS	[B/S]
,LTRIM(RTRIM('' )) AS	[CONTRA]
,SUM(CONVERT(MONEY,[Debit/Credit amount]) ) AS	[Debit/Credit amount]
, case when SUM(CONVERT(MONEY,[Debit/Credit amount]) )  >= 0 then 'D' else'C' end rdcd
,LTRIM(RTRIM(CCY )) AS	[CCY] 
,LTRIM(RTRIM('' )) AS	[CUSTOMER ADDRESS 1]
,LTRIM(RTRIM('' )) AS	[CUSTOMER ADDRESS 2]
,LTRIM(RTRIM('' )) AS	[CUSTOMER ADDRESS 3]
,LTRIM(RTRIM('' )) AS	[CUSTOMER ADDRESS 4]
,LTRIM(RTRIM('' )) AS	[CUSTOMER ADDRESS 5]
,LTRIM(RTRIM('CLOSE' )) AS	[TYPE]
,CONVERT(DECIMAL(16,4) ,0 ) AS	[MKT Price]
,CONVERT(MONEY, 0 ) AS	[MKT Value]
,90  AS	[orderby]
,LTRIM(RTRIM('' )) AS	[Cusip]
,LTRIM(RTRIM('' )) AS	[ISIN]

, ''[Contra Account Number] 
 ,'' [Name]
----select *
,1 as cnt 
from #activity
Where ISNULL(DESCRIPTION,'') <> 'CLOSING BALANCE' 
And exists  (select * from  ICBCFSAPPDB. [dbo].[Get_User_AccountAccesscontrol]  (@UserName)  C  where  entitycode =case when  C.Branch='ALL'  then entitycode else c.Branch  end 
                      and        trader=   case     when    c.AccountNo='ALL'   then    trader  else C.AccountNo  end  )
GROUp by LTRIM(RTRIM(Firmname )) --AS	[Firmname]
,LTRIM(RTRIM(entitycode )) --AS	[entitycode]
,LTRIM(RTRIM(CCY ))

order by entitycode, orderby, TYPE



Select entitycode ,ccy,[OPENING BALANCE] ,[CLOSING BALANCE] into #balance2
FROM
(
Select entitycode , [Description] ,[Debit/Credit amount],CCY from #final2 
where DESCRIPTION in ('OPENING BALANCE','CLOSING BALANCE' ,'') ) p
PIVOT ( SUM([Debit/Credit amount])
FOR [Description] in ([OPENING BALANCE] ,[CLOSING BALANCE])
) as PVT

--Select a.* , CAse when b.entitycode is null then 'Zero balance' else 'active balance' end 'balance_type'
--from #final2 a
-- join  ( Select entitycode,ISNULL(CCY,'USD') CCY from #balance2
--	--Where ISNULL( [OPENING BALANCE] ,0)  <> 0	or  ISNULL( [CLOSING BALANCE] ,0)  <> 0
--	) b on a.entitycode =b.entitycode and ISNULL(a.CCY,'USD')  =ISNULL(b.CCY,'USD') 
--	Where   a.entitycode = CASE 
--											WHEN @Correspondent = 'All' THEN a.entitycode 
--											ELSE @Correspondent 
--										  END 
--and a.trader = CASE 
--											WHEN @Trader = 'All' THEN a.trader 
--											ELSE @Trader 
--										  END 
--And a.CCY='USD'
--	order by a.entitycode, orderby, TYPE

	
	/**************************** FLIPPINF SIGN FOR CASH ACVITY AMOUNTS - 03/15/2024 ***************/
	SELECt 
	[Firmname],
a.[entitycode],
[trader],
[ACCOUNT NUMBER],
[impact trans num],
[EXT REF],
[SETT DT],
[Long/Short quantity]*-1   [Long/Short quantity],
[DESCRIPTION],
[CPN],
[MATURITY],
[REC/DEL],
[B/S],
[CONTRA],
[Debit/Credit amount] *-1 [Debit/Credit amount]  ,
[rdcd],
a.[CCY],
[CUSTOMER ADDRESS 1],
[CUSTOMER ADDRESS 2],
[CUSTOMER ADDRESS 3],
[CUSTOMER ADDRESS 4],
[CUSTOMER ADDRESS 5],
[TYPE],
[MKT Price],
[MKT Value] *-1  [MKT Value],
[orderby],
[Cusip],
[ISIN],
[Contra Account Number],
[Name],
[cnt]
, CAse when b.entitycode is null then 'Zero balance' else 'active balance' end 'balance_type'
from #final2 a
 join  ( Select entitycode,ISNULL(CCY,'USD') CCY from #balance2
	--Where ISNULL( [OPENING BALANCE] ,0)  <> 0	or  ISNULL( [CLOSING BALANCE] ,0)  <> 0
	) b on a.entitycode =b.entitycode and ISNULL(a.CCY,'USD')  =ISNULL(b.CCY,'USD') 
	Where   a.entitycode = CASE 
											WHEN @Correspondent = 'All' THEN a.entitycode 
											ELSE @Correspondent 
										  END 
and a.trader = CASE 
											WHEN @Trader = 'All' THEN a.trader 
											ELSE @Trader 
										  END 
And a.CCY='USD'
	order by a.entitycode, orderby, TYPE



Drop table if exists #temp ,#temp_fail ,#realtime , #activity ,#open ,#close ,#balances
DROP TABLE IF EXISTS #final1,#balance1
DROP TABLE IF EXISTS #final2,#balance2 ,#output

END



