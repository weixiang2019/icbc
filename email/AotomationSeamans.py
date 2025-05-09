import os
import shutil
#import pyodbc
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import traceback
from datetime import datetime, timedelta
import asyncio
import time

async def run_task():
    start_time = time.time()  # Get the start time
    total_runtime = 2 * 60 * 60  # 2 hours in seconds
    interval = 5 * 60  # 5 minutes in seconds
    us_custom_holidays = [
    "20250101",  # New Year's Day
    "20250120",  # Martin Luther King Jr. Day (Third Monday of January)
    "20250217",  # Presidents' Day (Third Monday of February)
    "20250418",  # good friday
    "20250526",  # Memorial Day (Last Monday of May)
    "20250619",  # Juneteenth National Independence Day
    "20250704",  # Independence Day
    "20250901",  # Labor Day (First Monday of September)
    "20251013",  # Columbus Day (Second Monday of October)
    "20251111",  # Veterans Day
    "20251127",  # Thanksgiving Day (Fourth Thursday of November)
    "20251225",  # Christmas Day
    ]


    us_holidays_set = set(us_custom_holidays)

    def is_today_holiday(holidays_list):
        today_str = datetime.today().strftime('%Y%m%d')
        return today_str in holidays_list

################## Check if today is a holiday
    if is_today_holiday(us_holidays_set):
        print('us holiday raise SystemExit ')
        raise SystemExit

    def get_previous_weekday(ref_date=None, holidays_list=None):
        if ref_date is None:
            ref_date = datetime.today() #datetime.today() #- timedelta(days=1)  #yesterday
        
        if holidays_list is None:
            holidays_list = set()  # Default empty set

        previous_day = ref_date - timedelta(days=1)
    
        while previous_day.weekday() > 4 or previous_day.strftime("%Y%m%d") in holidays_list:  
            previous_day -= timedelta(days=1)
        
        return previous_day

    while time.time() - start_time < total_runtime:
        print("Running task at:", time.strftime("%Y-%m-%d %H:%M:%S"))
        print(get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") )
        body = ""
        bodyBONY=""
        bodyintlops=""
        sender_email = "DONOTREPLY@ICBKFS.Com"
        smtp_server = "10.224.27.6" #"smtprelay.icbkfs.us.local" #"10.224.12.157" #"10.224.8.6"
        smtp_port = 25
        array_list_seamans_tprchs_sale_historical		= ["seamans_tprchs_sale_historical_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tacc_sec_hidr_memo		= ["tacc_sec_hldr_memo_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tsecurity_rating_extract		= ["tsecurity_rating_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tprchs_sale_trans_extract		= ["tprchs_sale_trans_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tsec_price_extract		= ["tsec_price_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tsec_trd_exchange_extract		= ["tsec_trd_exchange_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tsec_xref_key_extract		= ["tsec_xref_key_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tsecurity_desc_extract		= ["tsecurity_desc_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tprchs_sale_trans_COUNTS_extract		= ["tprchs_sale_trans_COUNTS_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tcurrency_cvrsn_extract		= ["tcurrency_cvrsn_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tdiv_trans_extract		= ["tdiv_trans_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_thouse_price_extract		= ["thouse_price_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tmgn_int_trans_extract		= ["tmgn_int_trans_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tmsd_base_extract		= ["tmsd_base_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tacc_party_extract		= ["tacc_party_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tacc_type_balance		= ["tacc_type_balance_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_taccount_sec_hidr_extract		= ["taccount_sec_hldr_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tbond_data_extract		= ["tbond_data_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tbook_trans_extract_with_account_freeze		= ["tbook_trans_extract_with_account_freeze_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tbook_trans_extract		= ["tbook_trans_extract" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_tcage_rdm_data_extract		= ["tcage_rdm_data_extract_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".txt"]
        array_list_APIBAL		= ["APIBAL_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".out"]
        array_list_APIBALA		= ["APIBALA_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".out"]
        array_list_APIBALE		= ["APIBALE_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".out"]
        array_list_po7583		= ["po7583" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%m.%d")]
        array_list_trn		= ["trn_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".dat"]
        array_list_trn3		= ["trn3_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".dat"]
        array_list_Euroclear = ["Euroclear - R30T-U - Securities balances - Real-time process - " + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".TXT"]
        array_list_Custody_Holdings = ["Custody_Holdings - " + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".csv"]
        array_list_OK = ["seaman_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".ok"] 
        array_list = ["seaman_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".err"] 

        source_folder = r"Y:\marginftp\BR"     
        #source_folder = r"c:\seamansdailyFiles" ##r"\\10.1.10.9\uatsftp\icbc_fs_br\IMPACT\ICBUAT\tmp\source"  
        folder_files = os.listdir(source_folder)
        folder_files_seamans_tprchs_sale_historical		=[file for file in 	array_list_seamans_tprchs_sale_historical	if file in folder_files] 
        folder_files_tacc_sec_hidr_memo		=[file for file in 	array_list_tacc_sec_hidr_memo	if file in folder_files] 
        folder_files_tsecurity_rating_extract		=[file for file in 	array_list_tsecurity_rating_extract	if file in folder_files] 
        folder_files_tprchs_sale_trans_extract		=[file for file in 	array_list_tprchs_sale_trans_extract	if file in folder_files] 
        folder_files_tsec_price_extract		=[file for file in 	array_list_tsec_price_extract	if file in folder_files] 
        folder_files_tsec_trd_exchange_extract		=[file for file in 	array_list_tsec_trd_exchange_extract	if file in folder_files] 
        folder_files_tsec_xref_key_extract		=[file for file in 	array_list_tsec_xref_key_extract	if file in folder_files] 
        folder_files_tsecurity_desc_extract		=[file for file in 	array_list_tsecurity_desc_extract	if file in folder_files] 
        folder_files_tprchs_sale_trans_COUNTS_extract		=[file for file in 	array_list_tprchs_sale_trans_COUNTS_extract	if file in folder_files] 
        folder_files_tcurrency_cvrsn_extract		=[file for file in 	array_list_tcurrency_cvrsn_extract	if file in folder_files] 
        folder_files_tdiv_trans_extract		=[file for file in 	array_list_tdiv_trans_extract	if file in folder_files] 
        folder_files_thouse_price_extract		=[file for file in 	array_list_thouse_price_extract	if file in folder_files] 
        folder_files_tmgn_int_trans_extract		=[file for file in 	array_list_tmgn_int_trans_extract	if file in folder_files] 
        folder_files_tmsd_base_extract		=[file for file in 	array_list_tmsd_base_extract	if file in folder_files] 
        folder_files_tacc_party_extract		=[file for file in 	array_list_tacc_party_extract	if file in folder_files] 
        folder_files_tacc_type_balance		=[file for file in 	array_list_tacc_type_balance	if file in folder_files] 
        folder_files_taccount_sec_hidr_extract		=[file for file in 	array_list_taccount_sec_hidr_extract	if file in folder_files] 
        folder_files_tbond_data_extract		=[file for file in 	array_list_tbond_data_extract	if file in folder_files] 
        folder_files_tbook_trans_extract_with_account_freeze		=[file for file in 	array_list_tbook_trans_extract_with_account_freeze	if file in folder_files] 
        folder_files_tbook_trans_extract		=[file for file in 	array_list_tbook_trans_extract	if file in folder_files] 
        folder_files_tcage_rdm_data_extract		=[file for file in 	array_list_tcage_rdm_data_extract	if file in folder_files] 
        folder_files_APIBAL		=[file for file in 	array_list_APIBAL	if file in folder_files] 
        folder_files_APIBALA		=[file for file in 	array_list_APIBALA	if file in folder_files] 
        folder_files_APIBALE		=[file for file in 	array_list_APIBALE	if file in folder_files] 
        folder_files_po7583		=[file for file in 	array_list_po7583	if file in folder_files] 
        folder_files_trn		=[file for file in 	array_list_trn	if file in folder_files] 
        folder_files_trn3		=[file for file in 	array_list_trn3	if file in folder_files] 
        folder_files_Euroclear = [file for file in array_list_Euroclear if file in folder_files]   
        folder_files_Custody_Holdings = [file for file in array_list_Custody_Holdings if file in folder_files] 

        source_folder = r"Y:\marginftp\Seamans"     
        #source_folder = r"c:\seamansdailyFiles" ##r"\\10.1.10.9\uatsftp\icbc_fs_br\IMPACT\ICBUAT\tmp\source"  
        folder_files = os.listdir(source_folder)
        folder_files_ok = [file for file in array_list_OK if file in folder_files]   
        folder_files_err = [file for file in array_list if file in folder_files]  






####################email  
        if folder_files_ok:
            body = body + "seaman_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".ok" + " is captured\n"
            subject = "Seamans Monitoring Alerts - [Success]"
            body = body + "\n" + "\n"+ "All 29 files arrived successfully!" + "\n" + "\n" +"ICBC Support team"
            print(body)
            recipient_emails = ["jim.lempenau@icbkfs.com" ,"Owen.Bolante@icbkfs.com","wei.xiang@icbkfs.com"]  #  #  BRSupport@icbkfs.com
            mailsubject= subject
            mailbody = body + "\n" + "\n" + "\n" + 'smtp_server:' + smtp_server
            message = MIMEMultipart()
            message['From'] = sender_email
            message['To'] = ', '.join(recipient_emails)
            message['Subject'] = mailsubject
            message.attach(MIMEText(mailbody, 'plain'))
        elif folder_files_err:
            subject = "Seamans Monitoring Alerts - [Error]"
            body = body + "seaman_" + get_previous_weekday(datetime.today(), us_holidays_set).strftime("%Y%m%d") + ".err" + " is captured\n"
            body ="\n" + "\n" + "\n" + "Hi " + "\n" + "\n" + f"Received an error for Seamans. " + "\n" + "\n" + "\n" + "\n"+ body+ "\n" + "\n"+ "\n" + "\n"  + "\n" + "\n" +"ICBC Support team"
            print(body)
            recipient_emails = ["jim.lempenau@icbkfs.com" ,"Owen.Bolante@icbkfs.com","wei.xiang@icbkfs.com"]  #  #  BRSupport@icbkfs.com
            mailsubject= subject
            mailbody = body + "\n" + "\n" + "\n" + 'smtp_server:' + smtp_server
            message = MIMEMultipart()
            message['From'] = sender_email
            message['To'] = ', '.join(recipient_emails)
            message['Subject'] = mailsubject
            message.attach(MIMEText(mailbody, 'plain'))
        else:
            if not folder_files_seamans_tprchs_sale_historical:
                body = body + " ".join(array_list_seamans_tprchs_sale_historical) + "\n"
            if not 	folder_files_tacc_sec_hidr_memo	:	
                body = body + " ".join(	array_list_tacc_sec_hidr_memo	) + "\n"
            if not 	folder_files_tsecurity_rating_extract	:	
                body = body + " ".join(	array_list_tsecurity_rating_extract	) + "\n"
            if not 	folder_files_tprchs_sale_trans_extract	:	
                body = body + " ".join(	array_list_tprchs_sale_trans_extract	) + "\n"
            if not 	folder_files_tsec_price_extract	:	
                body = body + " ".join(	array_list_tsec_price_extract	) + "\n"
            if not 	folder_files_tsec_trd_exchange_extract	:	
                body = body + " ".join(	array_list_tsec_trd_exchange_extract	) + "\n"
            if not 	folder_files_tsec_xref_key_extract	:	
                body = body + " ".join(	array_list_tsec_xref_key_extract	) + "\n"
            if not 	folder_files_tsecurity_desc_extract	:	
                body = body + " ".join(	array_list_tsecurity_desc_extract	) + "\n"
            if not 	folder_files_tprchs_sale_trans_COUNTS_extract	:	
                body = body + " ".join(	array_list_tprchs_sale_trans_COUNTS_extract	) + "\n"
            if not 	folder_files_tcurrency_cvrsn_extract	:	
                body = body + " ".join(	array_list_tcurrency_cvrsn_extract	) + "\n"
            if not 	folder_files_tdiv_trans_extract	:	
                body = body + " ".join(	array_list_tdiv_trans_extract	) + "\n"
            if not 	folder_files_thouse_price_extract	:	
                body = body + " ".join(	array_list_thouse_price_extract	) + "\n"
            if not 	folder_files_tmgn_int_trans_extract	:	
                body = body + " ".join(	array_list_tmgn_int_trans_extract	) + "\n"
            if not 	folder_files_tmsd_base_extract	:	
                body = body + " ".join(	array_list_tmsd_base_extract	) + "\n"
            if not 	folder_files_tacc_party_extract	:	
                body = body + " ".join(	array_list_tacc_party_extract	) + "\n"
            if not 	folder_files_tacc_type_balance	:	
                body = body + " ".join(	array_list_tacc_type_balance	) + "\n"
            if not 	folder_files_taccount_sec_hidr_extract	:	
                body = body + " ".join(	array_list_taccount_sec_hidr_extract	) + "\n"
            if not 	folder_files_tbond_data_extract	:	
                body = body + " ".join(	array_list_tbond_data_extract	) + "\n"
            if not 	folder_files_tbook_trans_extract_with_account_freeze	:	
                body = body + " ".join(	array_list_tbook_trans_extract_with_account_freeze	) + "\n"
            if not 	folder_files_tbook_trans_extract	:	
                body = body + " ".join(	array_list_tbook_trans_extract	) + "\n"
            if not 	folder_files_tcage_rdm_data_extract	:	
                body = body + " ".join(	array_list_tcage_rdm_data_extract	) + "\n"

            if body != "":
                subject = "Seamans Monitoring Alerts - [Missing EOD files]"
                body ="\n" + "\n" + "\n" + "Hello Ravi/Kushal," + "\n" + "\n" + f"It looks like these are these files are missing.  it should comes from the End Of Day message.  Could you assist in getting these files?" + "\n" + "\n" + "\n" + body + "\n" + "\n" + "\n" + "\n"  + "\n"  + "\n" +"ICBC Support team"
                print(body)
                recipient_emails = ["Ravi.Patel@icbkfs.com","brit@icbkfs.com","jim.lempenau@icbkfs.com" ,"Owen.Bolante@icbkfs.com","wei.xiang@icbkfs.com"]  #  #  BRSupport@icbkfs.com
                mailsubject= subject
                mailbody = body + "\n" + "\n" + "\n" + 'smtp_server:' + smtp_server
                message = MIMEMultipart()
                message['From'] = sender_email
                message['To'] = ', '.join(recipient_emails)
                message['Subject'] = mailsubject
                message.attach(MIMEText(mailbody, 'plain'))
                if message:
                    try: 
                        with smtplib.SMTP(smtp_server, smtp_port) as server:
                            if server.has_extn("STARTTLS"):
                                server.starttls() 
                            server.sendmail(sender_email, recipient_emails, message.as_string())
                            print('DW Email sent successfully!')
                            if folder_files_ok:
                                print('raise SystemExit')
                                raise SystemExit
                    except Exception as e:
                        print(f'Error sending DW email: {e}')



     ##########email to Grag for BONY            
            if not 	folder_files_APIBAL	:	
                bodyBONY = bodyBONY + " ".join(	array_list_APIBAL	) + "\n"
            if not 	folder_files_APIBALA	:	
                bodyBONY = bodyBONY + " ".join(	array_list_APIBALA	) + "\n"
            if not 	folder_files_APIBALE	:	
                bodyBONY = bodyBONY + " ".join(	array_list_APIBALE	) + "\n"
            if not 	folder_files_po7583	:	
                bodyBONY = bodyBONY + " ".join(	array_list_po7583	) + "\n"
            if not 	folder_files_trn	:	
                bodyBONY = bodyBONY + " ".join(	array_list_trn	) + "\n"
            if not 	folder_files_trn3	:	
                bodyBONY = bodyBONY + " ".join(	array_list_trn3	) + "\n"    

            if bodyBONY != "":
                subject = "Seamans Monitoring Alerts - [Missing BONY files]"
                bodyBONY ="\n" + "\n" + "\n" + "Hello Greg," + "\n" + "\n" + f"It looks like these are these files are missing.  it should comes from the BONY message.  Could you assist in getting these files?" + "\n" + "\n" + "\n" + bodyBONY + "\n" + "\n"  + "\n" + "\n"  + "\n"  + "\n" +"ICBC Support team"
                print(bodyBONY)
                recipient_emails = ["Greg.Stellate@icbkfs.com","jim.lempenau@icbkfs.com" ,"Owen.Bolante@icbkfs.com","wei.xiang@icbkfs.com"]  #  #  BRSupport@icbkfs.com
                mailsubject= subject
                mailbodyBONY = bodyBONY + "\n" + "\n" + "\n" + 'smtp_server:' + smtp_server
                message = MIMEMultipart()
                message['From'] = sender_email
                message['To'] = ', '.join(recipient_emails)
                message['Subject'] = mailsubject
                message.attach(MIMEText(mailbodyBONY, 'plain'))
                if message:
                    try: 
                        with smtplib.SMTP(smtp_server, smtp_port) as server:
                            if server.has_extn("STARTTLS"):
                                server.starttls() 
                            server.sendmail(sender_email, recipient_emails, message.as_string())
                            print('BONY Email sent successfully!')
                            if folder_files_ok:
                                print('raise SystemExit')
                                raise SystemExit
                    except Exception as e:
                        print(f'Error sending BONY email: {e}')



    #############################Euro custody sending email
            if not folder_files_Euroclear and not folder_files_Custody_Holdings:
                bodyintlops ="Euroclear securities balances and custody holdings"
            elif not folder_files_Euroclear:
                bodyintlops ="Euroclear securities balances"
            elif not folder_files_Custody_Holdings:
                bodyintlops ="custody holdings"

            if not folder_files_Euroclear or not folder_files_Custody_Holdings:
                subjectintlops = "Missing the " + bodyintlops + " file(s)  files for Seamans"
                bodyintlops ="\n" + "\n" + "\n" + "Hi intlops" + "\n" + "\n" + f"It looks like we are missing the " + bodyintlops + " file(s) for yesterday.  Would it be possible to add those? " + "\n" + "\n" + "\n" + "\n" + "\n" + "\n"+ "\n" + "\n" + "\n" + "\n" +"ICBC Support team"
                print(bodyintlops)
                recipient_emailsintlops = ["intlops@icbkfs.com","jim.lempenau@icbkfs.com" ,"Owen.Bolante@icbkfs.com","wei.xiang@icbkfs.com"]  #  #  BRSupport@icbkfs.com
                message = MIMEMultipart()
                message['From'] = sender_email
                message['To'] = ', '.join(recipient_emailsintlops)
                message['Subject'] = subjectintlops
                message.attach(MIMEText(bodyintlops, 'plain'))


        if folder_files_ok or folder_files_err or not folder_files_Euroclear or not folder_files_Custody_Holdings:
            if message:
                try: 
                    with smtplib.SMTP(smtp_server, smtp_port) as server:
                        if server.has_extn("STARTTLS"):
                            server.starttls() 
                        server.sendmail(sender_email, recipient_emails, message.as_string())
                        print('Email sent successfully!')
                        if folder_files_ok:
                            print('raise SystemExit')
                            raise SystemExit
                except Exception as e:
                    print(f'Error sending email: {e}')


        # Non-blocking sleep for 5 minutes
        await asyncio.sleep(interval)

    print("Task completed. Total runtime reached 2 hours.")

# Run the async function
asyncio.run(run_task())





