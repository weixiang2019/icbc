//------------------------------------------------------------------------------
// <copyright file="CSSqlStoredProcedure.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Messaging;
using System.Web;
using System.Net;
using System.IO;
using System.Text;
using System.Xml;
public partial class MSMQ_Procedure
{
    [Microsoft.SqlServer.Server.SqlProcedure]
    public static void ReceiveMSMQ(string path, string Label, string machinename,Int32 BodyType, int timeoutsec,  int transaction,  out string messageBody)
    {
        messageBody = "";

        MessageQueue remotePrivateQueue = new MessageQueue(path);
        MessageQueueTransaction myTransaction = new MessageQueueTransaction();
        Message oMessage = new Message();
        oMessage.BodyType = BodyType;
        // oMessage.Body = messageBody;
        // oMessage.Label = Label;


      //  remotePrivateQueue.MachineName = machinename;
        //  myTransaction.Begin();


        remotePrivateQueue.Formatter = new ActiveXMessageFormatter();
          try
        {

            if (transaction >= 1)
            {

                oMessage = remotePrivateQueue.Receive(TimeSpan.FromSeconds(timeoutsec), MessageQueueTransactionType.None);
                messageBody = oMessage.Body.ToString();
                messageBody = messageBody + "___" + oMessage.Id.ToString();

                myTransaction.Commit();
            }

            else
            {
                oMessage = remotePrivateQueue.Receive(TimeSpan.FromSeconds(timeoutsec));
                messageBody = oMessage.Body.ToString();
                messageBody = messageBody + "___" + oMessage.Id.ToString();
            }
              
              
              
           
        }
            catch (  Exception ex )
                {
                
                }

                
          





      
        // oMessage = remotePrivateQueue.Receive(MessageQueueTransactionType.Single);
       
        // myTransaction.Commit();

        //remotePrivateQueue.Close();

        // Put your code here
    }



   


    public static void SendMSMQ(string path, string messageBody, string Label, string machinename, Int32 BodyType,Int32 transaction ,out string Messageid)
    {
        //  MessageQueue remotePrivateQueue = new MessageQueue(path);
        MessageQueue remotePrivateQueue = new MessageQueue(path);
        Messageid = "send start";
        MessageQueueTransaction myTransaction = new MessageQueueTransaction();
        Message oMessage = new Message();
        oMessage.BodyType = BodyType;
        oMessage.Body = messageBody;
        oMessage.Label = Label;
        string exceptiontemp;

        remotePrivateQueue.Formatter = new ActiveXMessageFormatter();
        // remotePrivateQueue.MachineName = machinename;
        try
        {
            if (transaction >= 1)
            {

                myTransaction.Begin();

                remotePrivateQueue.Send(oMessage, myTransaction);
                Messageid = oMessage.Id;
                 
                myTransaction.Commit();
            
            
            }

            else

            {
                remotePrivateQueue.Send(oMessage, MessageQueueTransactionType.None);
                Messageid = oMessage.Id;
                Messageid = remotePrivateQueue.CanWrite.ToString ();
            }



        }
        catch (Exception ex)
        {
            exceptiontemp = ex.Message;


            Messageid = exceptiontemp;
        }

        remotePrivateQueue.Close();
      
        // Put your code here
    }



    public static void Status(string path, string messageBody, string Label, string machinename, Int32 BodyType, out string Status)
    {
        //  MessageQueue remotePrivateQueue = new MessageQueue(path);
        MessageQueue remotePrivateQueue = new MessageQueue(path);
         
        MessageQueueTransaction myTransaction = new MessageQueueTransaction();
        Message oMessage = new Message();
        oMessage.BodyType = BodyType;
        oMessage.Body = messageBody;
        oMessage.Label = Label;
        string exceptiontemp;
        // remotePrivateQueue.MachineName = machinename;
        try
        {
            myTransaction.Begin();

            exceptiontemp = "Read Status:" + remotePrivateQueue.CanRead.ToString() +"|";
            exceptiontemp = exceptiontemp + "Write Status:" + remotePrivateQueue.CanWrite.ToString();
            Status = exceptiontemp;

            myTransaction.Commit();
        }
        catch (Exception ex)
        {
            exceptiontemp = ex.Message;
            Status = exceptiontemp;
        }

        remotePrivateQueue.Close();

        // Put your code here
    }



    ////public static void ReceiveSecmaster(string cusip , string fieldname, out string value)
    ////{





    ////    value = callwebservice.webservice.receviesecmaster(cusip, fieldname);



    ////}
}
