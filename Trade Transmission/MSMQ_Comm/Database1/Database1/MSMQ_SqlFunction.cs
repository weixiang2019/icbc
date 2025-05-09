//------------------------------------------------------------------------------
// <copyright file="CSSqlFunction.cs" company="Microsoft">
//     Copyright (c) Microsoft Corporation.  All rights reserved.
// </copyright>
//------------------------------------------------------------------------------
using System;
using System.Data;
using System.Data.SqlClient;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;
using System.Messaging;
using System.Collections;

public partial class UserDefinedFunctions
{
   
    
    
    
    private class MQ_Messageproperty
    {
        public SqlString Message;
        public SqlString MessageID;
        public SqlDateTime CreationTime;
        public MQ_Messageproperty(SqlString fileName, SqlString messageID, DateTime creationTime)
        {
            Message = fileName;
            MessageID = messageID;
            CreationTime = creationTime;
        }
    }
    //The SqlFunction attribute tells Visual Studio to register this 
    //code as a user defined function
    [Microsoft.SqlServer.Server.SqlFunction(
        FillRowMethodName = "FindFiles",
        TableDefinition = "Message nvarchar(max) , MessageID  nvarchar(400),CreationTime date")]
    public static IEnumerable BuildFileNamesArray(string path, string machinename,Int32 timeSpanMinute)
    {

        string machinename2;
        machinename2 = machinename;

        
        MessageQueue remotePrivateQueue = new MessageQueue(path);
        ArrayList FilePropertiesCollection = new ArrayList();
        FilePropertiesCollection.Add(new MQ_Messageproperty("Message read started "  + remotePrivateQueue.CanRead.ToString(), "-1", DateTime.Now));
        try
        {
           



           

            //remotePrivateQueue.MachineName = machinename;

            //MessageQueueTransaction myTransaction = new MessageQueueTransaction();


            remotePrivateQueue.Formatter = new ActiveXMessageFormatter();

           remotePrivateQueue. MessageReadPropertyFilter.SetAll();

          
            //Message[] msgs = remotePrivateQueue.GetAllMessages();
            
           
            //  myTransaction.Begin();

            
            MessageEnumerator enumerator = remotePrivateQueue.GetMessageEnumerator2();
            Message oMessage = new Message();
           
             int a;


             for (a = 0; a < timeSpanMinute; a = a + 1)
             {
                  oMessage = remotePrivateQueue.Receive(MessageQueueTransactionType.None);

                  oMessage.BodyType = 8;
                  if (oMessage.Body.ToString () == "")
                  {
                      break; 
                  }
                        
                  FilePropertiesCollection.Add(new MQ_Messageproperty(oMessage.Body.ToString(),
                   oMessage.Id, oMessage.SentTime));
                   
             }
            
            
            
            //while (enumerator.MoveNext()==true )
            //{

            //    Message msg = enumerator.Current;
            //    msg.BodyType = 8;
            //   FilePropertiesCollection.Add(new MQ_Messageproperty(msg.Body.ToString(),
            //    msg.Id, msg.SentTime));
            //                    enumerator.RemoveCurrent();
            //}


            // oMessage = remotePrivateQueue.Receive(MessageQueueTransactionType.Single);


            //foreach (Message msg in msgs)
            //{

            //    //I'm adding to the colection the properties (FileProperties) 
            //    //of each file I've found  
            //    if (msg.SentTime > DateTime.Now.AddMinutes(-1 * timeSpanMinute))
            //    {
                     
            //        msg.BodyType = 8;
            //        FilePropertiesCollection.Add(new MQ_Messageproperty(msg.Body.ToString(),
            //        msg.Id, msg.SentTime));
                   
            //    }
            //}



           
        }
        catch (Exception ex)
        {
            FilePropertiesCollection.Add(new MQ_Messageproperty(ex.Message ,   "-1", DateTime.Now ));
            //return null;
        }

        return FilePropertiesCollection;

    }
    //FillRow method. The method name has been specified above as 
    //a SqlFunction attribute property
    public static void FindFiles(object objFileProperties, out SqlString Message,
    out SqlString MessageID, out SqlDateTime creationTime)
    {
        //I'm using here the FileProperties class defined above
        MQ_Messageproperty fileProperties = (MQ_Messageproperty)objFileProperties;
        Message = fileProperties.Message;
        MessageID = fileProperties.MessageID;
        creationTime = fileProperties.CreationTime;
    }
};