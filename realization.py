from airflow import DAG 

from datetime import timedelta, datetime 

from airflow.operators.python import PythonOperator

import boto3 

import vertica_python 

from vertica_python import connect 

DAG_ID='download_and_insert' 


with DAG( 

        dag_id= DAG_ID, 

        start_date=datetime(2022, 9, 24), 

        schedule_interval=timedelta(minutes=15), 

        catchup=False, 

) as dag: 

    def download(): 

        AWS_ACCESS_KEY_ID = "" 

        AWS_SECRET_ACCESS_KEY = "" 

        session = boto3.session.Session() 

        s3_client = session.client( 

            service_name='s3', 

            endpoint_url='', 

            aws_access_key_id=AWS_ACCESS_KEY_ID, 

            aws_secret_access_key=AWS_SECRET_ACCESS_KEY, 

        ) 

        s3_client.download_file( 

            Bucket='sprint6', 

            Key='file.csv', 

            Filename='/data/file.csv' 

        ) 

 

    def insert(): 

        conn_info = {'host': '', 

                    'port': '', 

                    'user': '', 

                    'password': '', 

                    'database': ''} 

        connect2= vertica_python.connect(**conn_info) 

        cur=connect2.cursor() 

        cur.execute('truncate table user__STAGING.group_log') 

        with open('/data/file.csv') as fs: 

            my_file = fs.read() 

            cur.copy("COPY user__STAGING.group_log FROM STDIN PARSER FDELIMITEDPARSER (delimiter=',')", my_file) 

            cur.close 

download_task=PythonOperator( 

    task_id='download', 

    python_callable=download, 

    dag=dag) 

 

insert_task=PythonOperator( 

    task_id='insert', 

    python_callable=insert, 

    dag=dag) 

 

download_task >> insert_task 
