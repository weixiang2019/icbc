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
using System.Text;
using System.Messaging;
using System.Text;
using System.Xml;
using System.IO;
 
using System.Net;
using System.Collections;
public partial class checksum
{
    [Microsoft.SqlServer.Server.SqlFunction]
    public static SqlString SqlFunction1(string messageinput)
    {
        Int32 total = 0;

        string message = messageinput;
        message = message.Replace('|', '\u0001');
        byte[] messageBytes = Encoding.ASCII.GetBytes(message);
        for (int i = 0; i < message.Length; i++)
            total += messageBytes[i];

        int checksum = total % 256;
        string checksumStr = checksum.ToString().PadLeft(3, '0');
        // Put your code here
        return checksumStr;
    }


    public static SqlString fn_getmsmqcount(string path)
    {
        
        MessageQueue remotePrivateQueue = new MessageQueue(path);
               
          Message[] msgs = remotePrivateQueue.GetAllMessages();

          int count = 0;

          foreach (Message msg in msgs)
          {

              //I'm adding to the colection the properties (FileProperties) 
              //of each file I've found  
              count++;

          }


        // Put your code here
          return count.ToString ();
    }

    public static SqlString fn_PurgeMQ(string path)
    {

        MessageQueue remotePrivateQueue = new MessageQueue(path);

        remotePrivateQueue.Purge();
           


        // Put your code here
        return path;
    }





    public static SqlString fn_receivemessage(string path, string Label, string machinename, Int32 BodyType, int timeoutsec, int transaction )
    {
        string messageBody;
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

            if (transaction != 0)
            {
                myTransaction.Begin();
                if (timeoutsec == 0)
                {
                    oMessage = remotePrivateQueue.Receive( MessageQueueTransactionType.None);
                }

                if (timeoutsec != 0)
                {
                    oMessage = remotePrivateQueue.Receive(TimeSpan.FromSeconds(timeoutsec), MessageQueueTransactionType.None);
                }
                    messageBody = oMessage.Body.ToString();
                messageBody = messageBody + "___" + oMessage.Id.ToString();

                myTransaction.Commit();
            }

            else
            {

                if (timeoutsec == 0)
                {
                    oMessage = remotePrivateQueue.Receive( );

                }

                if (timeoutsec != 0)
                {
                    oMessage = remotePrivateQueue.Receive(TimeSpan.FromSeconds(timeoutsec));

                }
                      messageBody = oMessage.Body.ToString();
                messageBody = messageBody + "___" + oMessage.Id.ToString();
            }




        }
        catch (Exception ex)
        {

        }









        // oMessage = remotePriva
        return messageBody;
    }


    public static HttpWebRequest CreateWebRequest(string URL)
    {
        HttpWebRequest webRequest = (HttpWebRequest)WebRequest.Create(URL);
        webRequest.Headers.Add(@"SOAP:Action");
        webRequest.ContentType = "text/xml;charset=\"utf-8\"";
        webRequest.Accept = "text/xml";
        webRequest.Method = "POST";
        return webRequest;
    }


    public static void Executewrite(string URL,String queueManager, String queueName, string strmessage)
    {
        var fixStr = strmessage;

        HttpWebRequest request = CreateWebRequest(URL);
        XmlDocument soapEnvelopeXml = new XmlDocument();
        soapEnvelopeXml.LoadXml(@"<?xml version=""1.0"" encoding=""utf-8""?>
<soap:Envelope xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" xmlns:soap=""http://schemas.xmlsoap.org/soap/envelope/"">
  <soap:Body><sendMessageLocal xmlns=""http://tempuri.org/""> <queueManager>" + queueManager + "</queueManager><queueName>" + queueName + "</queueName><strmessage>" + fixStr + "</strmessage></sendMessageLocal></soap:Body></soap:Envelope>");

        using (Stream stream = request.GetRequestStream())
        {
            soapEnvelopeXml.Save(stream);
        }

        using (WebResponse response = request.GetResponse())
        {
            using (StreamReader rd = new StreamReader(response.GetResponseStream()))
            {
                string soapResult = rd.ReadToEnd();
                Console.WriteLine(soapResult);
            }
        }
    }

    public static string Executeread(string URL,String queueManager, String queueName )
    {


        HttpWebRequest request = CreateWebRequest( URL);
        XmlDocument soapEnvelopeXml = new XmlDocument();
        soapEnvelopeXml.LoadXml(@"<?xml version=""1.0"" encoding=""utf-8""?>
<soap:Envelope xmlns:xsi=""http://www.w3.org/2001/XMLSchema-instance"" xmlns:xsd=""http://www.w3.org/2001/XMLSchema"" xmlns:soap=""http://schemas.xmlsoap.org/soap/envelope/"">
  <soap:Body><ReceiveMessageLocal xmlns=""http://tempuri.org/""> <queueManager>" + queueManager + "</queueManager><queueName>" + queueName + "</queueName></ReceiveMessageLocal></soap:Body></soap:Envelope>");

        using (Stream stream = request.GetRequestStream())
        {
            soapEnvelopeXml.Save(stream);
        }

        using (WebResponse response = request.GetResponse())
        {
            using (StreamReader rd = new StreamReader(response.GetResponseStream()))
            {
                string soapResult = rd.ReadToEnd();
                return soapResult;
            }
        }
    }

    public static SqlString IBMMQSendMessage(String queueManager, String queueName, string strmessage)
    {


        string ReturnValue;
        ReturnValue = "";
        Executewrite(@"http://icbcfslbr8/MQwebservice/WBSMQ.asmx?op=sendMessageLocal",queueManager, queueName, strmessage);
        ////HttpWebRequest request = (HttpWebRequest)WebRequest.Create("http://icbcfslbr8/MQwebservice/WBSMQ.asmx?op=sendMessageLocal&queueManager=" + queueManager + "&queueName=" + queueName + "&strmessage=" + strmessage);

        ////request.Method = "POST";
        ////request.ContentLength = 0;
        ////request.Credentials = CredentialCache.DefaultCredentials;
        ////HttpWebResponse response = (HttpWebResponse)request.GetResponse();
        ////Stream receiveStream = response.GetResponseStream();
        ////// Pipes the stream to a higher level stream reader with the required encoding format.   
        
        ////StreamReader readStream = new StreamReader(receiveStream, Encoding.UTF8);

        ////Console.WriteLine("Response stream received.");
        ////System.IO.File.WriteAllText("E:\\CLR\\Response\\response.txt", readStream.ReadToEnd());   

        ////response.Close();
        ////readStream.Close();  


        return ReturnValue;

     }



    public static SqlString IBMMQReadMessage(String queueManager, String queueName )
    {


        string ReturnValue;
        ReturnValue = "";
        ReturnValue = Executeread(@"http://icbcfslbr8/MQwebservice/WBSMQ.asmx?op=ReceiveMessageLocal", queueManager, queueName);
        ////HttpWebRequest request = (HttpWebRequest)WebRequest.Create("http://icbcfslbr8/MQwebservice/WBSMQ.asmx?op=sendMessageLocal&queueManager=" + queueManager + "&queueName=" + queueName + "&strmessage=" + strmessage);

        ////request.Method = "POST";
        ////request.ContentLength = 0;
        ////request.Credentials = CredentialCache.DefaultCredentials;
        ////HttpWebResponse response = (HttpWebResponse)request.GetResponse();
        ////Stream receiveStream = response.GetResponseStream();
        ////// Pipes the stream to a higher level stream reader with the required encoding format.   

        ////StreamReader readStream = new StreamReader(receiveStream, Encoding.UTF8);

        ////Console.WriteLine("Response stream received.");
        ////System.IO.File.WriteAllText("E:\\CLR\\Response\\response.txt", readStream.ReadToEnd());   

        ////response.Close();
        ////readStream.Close();  


        return ReturnValue;

    }


    public static SqlString fn_XML_Translate(string XML, string ColumnNameSerator, string ColumnSerator)
    {

         string xml;
         xml = XML;
        ////string ColumnNameSerator;
        ////string ColumnSerator;
        ////ColumnNameSerator = ":";

        ////ColumnSerator = "~";

        string xmlschema;
       


        XmlDocument doc = new XmlDocument();

        
        byte[] buffer = Encoding.UTF8.GetBytes(xml);
        DataSet ds = new DataSet();

        //using (MemoryStream stream = new MemoryStream(bufferxmlschema))
        //{
        //    XmlReader reader = XmlReader.Create(stream);
        //    ds.ReadXmlSchema(reader);

        //}



        using (MemoryStream stream = new MemoryStream(buffer))
        {
            XmlReader reader = XmlReader.Create(stream);
            ds.ReadXml(reader);

        }

        DataTable finaldt = new DataTable();
        finaldt = ds.Tables[0];
        int tabid;
        tabid = 0;

        DataSet cloneSet = new DataSet();
        for (int i3 = 0; i3 < ds.Tables.Count; i3++)
        {
            cloneSet.Tables.Add(ds.Tables[i3].TableName);
            for (int i4 = 0; i4 < ds.Tables[i3].Columns.Count; i4++)
            {
                cloneSet.Tables[i3].Columns.Add(ds.Tables[i3].Columns[i4].ColumnName, typeof(string));


            }

        }


        ////using (MemoryStream stream = new MemoryStream(buffer))
        ////{
        ////    XmlReader reader = XmlReader.Create(stream);
        ////    cloneSet.ReadXml(reader);

        ////}

        //foreach (DataTable table in ds.Tables)
        //{
        //    if (tabid > 0)
        //    {
        //        if (table.Rows.Count > 1)
        //        {
        //            ////''finaldt.Merge(table);
        //            ds.Tables[0].Merge(ds.Tables[tabid]);
        //        }
        //    }

        //    tabid = tabid + 1;
        //}

       
        ///add entries 

        for (int i5 = 0; i5 < ds.Tables.Count; i5++)
        {
            foreach (DataRow dr in ds.Tables[i5].Rows)
            {

                cloneSet.Tables[i5].Rows.Add(dr.ItemArray);
            }

        }

        if (ds.Tables.Count > 1)
        {
            /// update first record 
            for (int i7 = 0; i7 < ds.Tables.Count; i7++)
            {
                for (int i = 0; i < cloneSet.Tables[i7].Rows.Count; i++)
                {
                    for (int i2 = 0; i2 < cloneSet.Tables[i7].Columns.Count; i2++)
                    {

                        string col = cloneSet.Tables[i7].Columns[i2].ColumnName;


                        if (i < 1)
                        {
                            cloneSet.Tables[i7].Rows[0][col] = ds.Tables[i7].Rows[i][col];
                        }

                        if (i > 0)
                        {
                            cloneSet.Tables[i7].Rows[0][col] = cloneSet.Tables[i7].Rows[0][col] + "|" + ds.Tables[i7].Rows[i][col];

                        }

                    }

                    if (i > 1)
                    {
                        cloneSet.Tables[i7].Rows[i - 1].Delete();
                    }

                }

            }
            /// delete extra records 

            for (int i6 = 0; i6 < ds.Tables.Count; i6++)
            {
                if (cloneSet.Tables[i6].Rows.Count > 1)
                {


                    for (int i7 = 0; i7 < cloneSet.Tables[i6].Rows.Count; i7++)
                    {
                        if (i7 > 0)
                        {
                            cloneSet.Tables[i6].Rows[i7].Delete();
                        }

                    }
                }
            }

        }

        // create csv format message
        string finalvalue;
        finalvalue = "";
        for (int i5 = 0; i5 < cloneSet.Tables.Count; i5++)
        {



            foreach (DataRow dr in cloneSet.Tables[i5].Rows)
            {


                for (int I = 0; I < cloneSet.Tables[i5].Columns.Count; I++)
                {
                    string col2 = cloneSet.Tables[i5].Columns[I].ColumnName;
                    finalvalue = finalvalue + cloneSet.Tables[i5].TableName + "_" + col2 + ColumnNameSerator + dr[col2] + ColumnSerator;


                }

            }


            //if in convert list then convert rows to column 




        }

        return finalvalue;
        //merge all table 

        ////foreach (DataTable table in cloneSet.Tables)
        ////{
        ////    if (tabid > 0)
        ////    {
        ////        if (table.Rows.Count > 1)
        ////        {
        ////            //''finaldt.Merge(table);
        ////            cloneSet.Tables[0].Merge(ds.Tables[tabid]);
        ////        }
        ////    }

        ////    tabid = tabid + 1;
        ////}


        // ds.Tables[0].Merge(ds.Tables[1]);
        //ds.Tables[0].Merge(ds.Tables[2]);
        //ds.Tables[0].Merge(ds.Tables[3]);
        //ds.Tables[0].Merge(ds.Tables[4]);
        //  ds.Tables[0].Merge(ds.Tables[1]);




    }



    public static SqlString fn_XML_Translate_singletable(string XML, string ColumnNameSerator, string ColumnSerator)
    {

        string xml;
        xml = XML;
        ////string ColumnNameSerator;
        ////string ColumnSerator;
        ////ColumnNameSerator = ":";

        ////ColumnSerator = "~";

        string xmlschema;



        XmlDocument doc = new XmlDocument();


        byte[] buffer = Encoding.UTF8.GetBytes(xml);
        DataSet ds = new DataSet();

        //using (MemoryStream stream = new MemoryStream(bufferxmlschema))
        //{
        //    XmlReader reader = XmlReader.Create(stream);
        //    ds.ReadXmlSchema(reader);

        //}



        using (MemoryStream stream = new MemoryStream(buffer))
        {
            XmlReader reader = XmlReader.Create(stream);
            ds.ReadXml(reader);

        }

        //DataTable finaldt = new DataTable();
        //finaldt = ds.Tables[0];
        //int tabid;
        //tabid = 0;

        //DataSet cloneSet = new DataSet();
        //for (int i3 = 0; i3 < ds.Tables.Count; i3++)
        //{
        //    cloneSet.Tables.Add(ds.Tables[i3].TableName);
        //    for (int i4 = 0; i4 < ds.Tables[i3].Columns.Count; i4++)
        //    {
        //        cloneSet.Tables[i3].Columns.Add(ds.Tables[i3].Columns[i4].ColumnName, typeof(string));


        //    }

        //}


        // create csv format message
        string finalvalue;
        finalvalue = "";
        for (int i5 = 0; i5 < ds.Tables.Count; i5++)
        {



            foreach (DataRow dr in ds.Tables[i5].Rows)
            {


                for (int I = 0; I < ds.Tables[i5].Columns.Count; I++)
                {
                    string col2 = ds.Tables[i5].Columns[I].ColumnName;
                    finalvalue = finalvalue +  "_" + col2 + ColumnNameSerator + dr[col2] + ColumnSerator;


                }

            }


            //if in convert list then convert rows to column 




        }




        return finalvalue;
        //merge all table 

        ////foreach (DataTable table in cloneSet.Tables)
        ////{
        ////    if (tabid > 0)
        ////    {
        ////        if (table.Rows.Count > 1)
        ////        {
        ////            //''finaldt.Merge(table);
        ////            cloneSet.Tables[0].Merge(ds.Tables[tabid]);
        ////        }
        ////    }

        ////    tabid = tabid + 1;
        ////}


        // ds.Tables[0].Merge(ds.Tables[1]);
        //ds.Tables[0].Merge(ds.Tables[2]);
        //ds.Tables[0].Merge(ds.Tables[3]);
        //ds.Tables[0].Merge(ds.Tables[4]);
        //  ds.Tables[0].Merge(ds.Tables[1]);




    }

      public static SqlString fn_sendmessage(string path, string messageBody, string Label, string machinename, Int32 BodyType,Int32 transaction  )
    {
          string Messageid ;
        //  MessageQueue remotePrivateQueue = new MessageQueue(path);

        Messageid = "intiated";

          MessageQueue remotePrivateQueue = new MessageQueue(path);
        Messageid = "-" + remotePrivateQueue.CanWrite.ToString();

        

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
            }



        }
        catch (Exception ex)
        {
            exceptiontemp = ex.Message;
            Messageid = Messageid+exceptiontemp.ToString ();
        }

        remotePrivateQueue.Close();
       
        return Messageid;
        // Put your code here
    }






      public static SqlString fn_sendmessage_local(string path, string messageBody, string Label, string machinename, Int32 BodyType, Int32 transaction)
      {
          string Messageid;
          //  MessageQueue remotePrivateQueue = new MessageQueue(path);



          MessageQueue remotePrivateQueue = new MessageQueue(path);



          Messageid = "";
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
              }



          }
          catch (Exception ex)
          {
              exceptiontemp = ex.Message;

          }

          remotePrivateQueue.Close();

          return Messageid;
          // Put your code here
      }


    




}
