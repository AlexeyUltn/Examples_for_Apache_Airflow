import requests 

import json 

 

headers ={"X-API-KEY": "", 

          "X-Nickname": ""} 

 

get_restourants = requests.get( 

    f"https://.json", 

    headers=headers 

) 

result_restourants=(json.dumps(get_restourants.json())) 

 

get_couriers = requests.get( 

    f"https://.json", 

    headers=headers 

) 

result_couriers = (json.dumps(get_couriers.json())) 

 

get_deliveries = requests.get( 

    f"https://.json", 

    headers=headers 

) 

result_deliveries = (json.dumps(get_deliveries.json())) 

 

## Наполнение таблиц в stg 

import psycopg2 

from airflow.operators.python import PythonOperator 

from datetime import timedelta, datetime 

from airflow import DAG 

import logging 

 

DAG_ID1 = 'to_stg' 

 

with DAG( 

        dag_id=DAG_ID1, 

        start_date=datetime(2022, 2, 2), 

        schedule_interval=timedelta(minutes=15), 

        catchup=False, 

) as dag: 

    def to_stg(): 

        try: 

            connection = psycopg2.connect(database='', host='', port='', password='', user='') 

            cur = connection.cursor() 

            query01 = 'truncate table stg.restourants' 

            query02 = 'truncate table stg.couriers' 

            query03 = 'truncate table insert into stg.deliveries' 

            cur.execute(query01) 

            cur.execute(query02) 

            cur.execute(query03) 

            restourants_row = json.loads(result_restourants) 

            for row in restourants_row: 

                id = row['_id'] 

                name = row['name'] 

                insert_row1 = (id, name) 

                insert_query1 = 'insert into stg.restourants ("id", "name") values (%s, %s)' 

                cur.execute(insert_query1, insert_row1) 

 

            couriers_row = json.loads(result_couriers) 

            for row in couriers_row: 

                id = row['_id'] 

                name = row['name'] 

                insert_row2 = (id, name) 

                insert_query2 = 'insert into stg.couriers ("id", "name") values (%s, %s)' 

                cur.execute(insert_query2, insert_row2) 

 

            deliveries_row = json.loads(result_deliveries) 

            for row in deliveries_row: 

                order_id = row['order_id'] 

                order_ts = row['order_ts'] 

                delivery_id = row['delivery_id'] 

                courier_id = row['courier_id'] 

                address = row['address'] 

                delivery_ts = row['delivery_ts'] 

                rate = row['rate'] 

                sum = row['sum'] 

                tip_sum = row['tip_sum'] 

                insert_row3 = (order_id, order_ts, delivery_id, courier_id, address, delivery_ts, rate, sum, tip_sum) 

                insert_query3 = 'insert into stg.deliveries ("order_id", "order_ts", "delivery_id", "courier_id", "address", "delivery_ts", "rate", "sum", "tip_sum") values (%s, %s, %s, %s, %s, %s, %s, %s, %s)' 

                cur.execute(insert_query3, insert_row3) 

            cur.commit() 

            cur.close() 

        except Exception as a: 

            logging.error("Exception occurred", exc_info=True) 

 

insert_task_to_stg = PythonOperator( 

    task_id='transform_task', 

    python_callable=to_stg, 

    dag=dag) 

 

insert_task_to_stg 
