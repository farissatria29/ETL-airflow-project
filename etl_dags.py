from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
import traceback

default_args = {
    'owner': 'your_name',
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_etl_script(**kwargs):
    try:
        import pandas as pd
        from sqlalchemy import create_engine
        import os

        engine_pb_db = create_engine('mysql+mysqlconnector://ibnu_data_popbox_db:ibnu90oikjn><$@35.213.166.160/popbox_db')
        engine_local_pb = create_engine('mysql+mysqlconnector://root:genK!j0data@127.0.0.1/popbox')

        ### Read Locker Location Grouping Data
        locker_location_id = pd.read_csv(r'G:\.shortcut-targets-by-id\1z28M3WkZT7tc9qr7_rpABaOlgWbQ6zEe\Data Project\Locker Location Grouping Data.csv', sep=';')
        locker_location_id = locker_location_id[['Locker ID','Name']]


        
        ### Read Data Locker Location Grouping
        location_group = pd.read_csv(r'G:\Locker Location Grouping Data.csv', sep=';')
        location_group = location_group[['Locker ID','Building Type','Location Group','Developer/Pengembang']]

        ### Read CSV
        otp = pd.read_csv(r'G:\OTP WA.csv',sep=",", parse_dates=['created_at'])
        otp = otp.drop(columns='Unnamed: 0')

        last_date = (otp['created_at'].max() + timedelta(1)).strftime('%Y-%m-%d')
        today = datetime.now().strftime('%Y-%m-%d')

        otp_top = otp #[otp['created_at'] < last_date]

        ### Read Database

        ### OTP Whatsapp Delivered
        query_pb_db=f''' 
        select 
            nexmo.*, partner.message_type,partner.locker_id, partner.template, partner.type, partner.client_id
        from 
            popbox_db.nexmo_whatsapp_logs nexmo
        left join
            popbox_db.partner_whatsapp_requests partner ON nexmo.message_uuid = partner.message_uuid
        where
            nexmo.status = 'delivered'
            and (nexmo.created_at between '{last_date} 00:00:00' and '{today} 00:00:00')
        '''
        otp_bot_1=pd.read_sql_query(query_pb_db,engine_pb_db)
        engine_pb_db.dispose()
        # otp_bot_1['MonthYear'] = otp_bot_1.apply(bulan_tahun, axis=1)
        otp_bot_1['created_at'] = pd.to_datetime(otp_bot_1['created_at'],format='%Y-%m-%d %H:%M:%S')
        otp_bot_1['MonthYear']=otp_bot_1['created_at'].map(lambda x: x.strftime('%Y-%m'))

        ### Template Dictionary
        service = {
            "INSULATOR" : [
                "ins_prod_v1",
                "ins_prod_v2",
                "ins_prod_v3"
                ],

            "LAST MILE" : [
                "dhl_v1",
                "kode_buka_prod_v10",
                "kode_buka_prod_v11",
                "kode_buka_prod_v12",
                "kode_buka_prod_v13",
                "kode_buka_prod_v3",
                "kode_buka_prod_v4",
                "kode_buka_prod_v6",
                "kode_buka_prod_v7",
                "kode_buka_prod_v8",
                "kode_buka_prod_v9",
                "last_mile_01",
                "lastmile_202107",
                "lastmile_23april2021",
                "lastmile_pincode_202108",
                "lastmile_pincode_global_v01",
                "lastmile_pincode_global_v04",
                "lastmile_pincode_global_v05",
                "lastmile_pincode_global_v06",
                "lastmile_pincode_global_v07",
                "lastmile_pincode_global_v09",
                "lastmile_pincode_global_v10",
                "lastmile_pincode_ppc_non_free_v01",
                "lastmile_pincode_ppc_non_free_v02",
                "lastmile_pincode_ppc_non_free_v05",
                "lastmile_pincode_ppc_non_free_v06",
                "lastmile_pincode_ppc_non_free_v07",
                "lastmile_pincode_ppc_non_free_v08",
                "lastmile_pincode_ppc_with_free_v01",
                "lastmile_pincode_ppc_with_free_v03",
                "lastmile_pincode_ppc_with_free_v05",
                "lastmile_pincode_ppc_with_free_v06",
                "lastmile_pincode_ppc_with_free_v07",
                "lastmile_pincode_ppc_with_free_v08",
                "lastmile_pincode_ppc1A_202108",
                "lastmile_pincode_ppc2A_202108",
                "popbox_5_anniv",
                "popbox_5_anniv_v2",
                "testing_qr",
                "testing_qr2",
                "testing_qr3",
                "lastmile_pincode_ppc_with_free_v10",
                "lastmile_pincode_ppc_non_free_v11",
                ],

            "ON DEMAND" : [
                "pod_payment_receipt_202107",
                "pod_payment_receipt_v1"
                ],

            "OTHERS" : [
                "oriflame_pex",
                "oriflame_pex2",
                "pod_202107"
                ],

            "POPSAFE" : [
                "popsafe",
                "popsafe_reminder_202108",
                "popsafe_v2",
                "popsafe_v3",
                "popsafe_v5_marketing",
                "popsafe_v6_marketing",
                "popsafe_v8_marketing"
                ],


            "RETURN" : [
                "rayspeed",
                "rayspeed_v3",
                "return",
                "return_v2",
                "zalora_return",
                "zalora_return_v3"
                ]
        }

        inp = otp_bot_1['template']
        serv = []

        for index, row in otp_bot_1.iterrows():
            template = row['template']
            found_in_service = False

            for key, val in service.items():
                if template in val:
                    serv.append(key)
                    found_in_service = True
                    break
            if not found_in_service:
                if pd.isna(row['locker_id']):
                    serv.append('OTP Apps')
                else:
                    if row['client_id'] == "PBXOTP":
                        serv.append('OTP Locker')
                    else:
                        serv.append('OTHERS')
        
        otp_bot_1['service'] = serv
        otp_bot = pd.merge(otp_bot_1, location_group, how='left', left_on='locker_id', right_on='Locker ID')
        otp_bot_2 = otp_bot.drop(columns='client_id')

        ### Merge all dataframe into 1 dataframe
        otp_bot_2['week'] = otp_bot_2['created_at'].apply(lambda x : str(x.isocalendar().week) if x.isocalendar().week >= 10 else "0"+str(x.isocalendar().week))
        otp_bot_2['year_iso'] = otp_bot_2['created_at'].dt.isocalendar().year
        otp_bot_2['week_year'] = otp_bot_2['year_iso'].astype("str") + "-W" + otp_bot_2['week']
        otp_final_1 = pd.merge(otp_bot_2, locker_location_id, how='left', left_on='locker_id', right_on='Locker ID')
        otp_final_1 = otp_final_1.drop(columns='Locker ID_y')
        otp_final_1 = otp_final_1.rename(columns={'Locker ID_x' : 'Locker ID'})
        otp_final_2 = pd.concat([otp_top, otp_final_1])

        ### Save Final Dataframe to csv
        otp_final_2.to_csv('G:\My Drive\PB WA Analisa Data\Data\OTP WA.csv', sep=",")

        ### Split Dataframe per 5000 row
        otp_sql = []
        j = 0
        k = 5000
        g = 5000
        for i in range(len(otp_final_2)):
            if j <= len(otp_final_2):
                otp_i = otp_final_2[j:k]
                j = j + g
                k = k + g
                otp_sql.append(otp_i)
            else :
                break

        ### Push Dataframe to sql
        error_otp = []
        m = 0
        for l in otp_sql :
            try :
                if m == 0 :
                    l.to_sql('otp', con=engine_local_pb, if_exists='replace', index=False)
                else :
                    l.to_sql('otp', con=engine_local_pb, if_exists='append', index=False)
                m = m + 1
            # except Exception as Argument:
            except :
                m = m + 1
                error_otp.append(l)
    
                # creating/opening a file
                f = open("logeror_auto_otpwa.txt", "a")
            
                # writing in the file
                #  f.write(str(datetime.now())+" "+str(Argument)+'\n')
                # f.write((datetime.now()).strftime('%Y-%m-%d %H:%M:%S')+" "+str(Argument)+'\n')
                f.write((datetime.now()).strftime('%Y-%m-%d %H:%M:%S')+" "+str(traceback.format_exc())+'\n')

                # closing the file
                f.close()
        
        if len(error_otp) != 0:
            error_otp = pd.concat(error_otp)
            error_otp.to_csv('Error OTP.csv')
        else:
            pass
        
        dir_path = os.path.dirname(os.path.realpath(__file__))
        filename = os.path.join(dir_path, 'auto_otpwa_log.log')

        # Logger
        logger = kwargs['ti'].log

        def do_logging():
            logger.info("update")

        if __name__ == '__main__':
            do_logging()

        logger.info("ETL script executed successfully.")

    except Exception as e:
        # Log any errors using Airflow's logger
        logger.error(f"Error in ETL script: {str(e)}")
        # Log the traceback for more detailed error information
        logger.error(traceback.format_exc())
        raise

dag = DAG(
    'your_etl_dag',
    default_args=default_args,
    description='Your ETL DAG',
    schedule_interval=timedelta(days=1),
)

# Define the ETL task
etl_task = PythonOperator(
    task_id='run_etl_script',
    python_callable=run_etl_script,
    provide_context=True,
    dag=dag,
)