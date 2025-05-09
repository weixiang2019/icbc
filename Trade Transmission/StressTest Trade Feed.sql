--select * from tradeloadstaging.dbo.trade_gui order by 1 desc
--select top 100 * from TradeLoadstaging.dbo.GemsAckNACK order by TransactTime desc --where execid <>'EFBVG137' 

use [TradeLoadstaging]
DECLARE @counter INT = 1,@receivemessagekeyid int =2890;
	--begin tran
    WHILE @counter <= 1000
    BEGIN

DECLARE @recvedmessagekey  int, @system nvarchar(50),  @processtype nvarchar(20)
select @recvedmessagekey=receivemessagekeyid,@system=system, @processtype=processtype    from    [dbo].[MessageInbound] where receivemessagekeyid = @receivemessagekeyid  --2758

begin try
--	exec [dbo].[sp_Load_OutrightTrades_realtime_oneby_one] @recvedmessagekey,@system,@processtype
begin tran tran1 
	exec ServiceBroker.sp_serviceBroker_add_trade   @recvedmessagekey,@system,@processtype
	--  insert into servicebroker.[MessageInbound_audit]  select @recvedmessagekey,convert(datetime,getdate(),101),@processtype, 'Added in Service Broker' ,getdate(),convert(Datetime,getdate(),101)
 commit tran tran1 
 end try
  
  begin catch
 
    insert into servicebroker.[MessageInbound_audit]  select @recvedmessagekey,convert(datetime,getdate(),101),@processtype, 'Error send Service Broker' ,getdate(),convert(Datetime,getdate(),101)
	   rollback tran tran1 
  end catch
  update a  set   a.source_BRID=
  --select TradeID,*,--isnull(a.source_BRID,
 -- case when isnull(a.previous_BrExecID,'')<>''  then  null else 
          CONVERT(NVARCHAR(500), 'E'+   dbo.fn_Return_AlphaValue([Trade Date])+Isnull( CONVERT(  NVARCHAR(8) 
         , a.tradeid, 101), ''), 101)      from dbo.Trade a   where TradeID =SCOPE_IDENTITY()--@tradeid
		 and  isnull(a.source_BRID,'')=''
  --Update Trade set [source_BRID] ='EFBVG' + Convert(nvarchar(50),@counter)    where TradeID =SCOPE_IDENTITY()
          
      SET @counter = @counter + 1;
    END;
		  select * from tradeloadstaging.dbo.trade order by TradeID desc
	 -- rollback tran