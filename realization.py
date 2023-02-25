import airflow 

from airflow import DAG 

from airflow.operators.bash import BashOperator 

from datetime import datetime 

from datetime import timedelta 

from airflow.operators.python import PythonOperator 

import os 

 

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf' 

os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf' 

os.environ['JAVA_HOME']='/usr' 

os.environ['SPARK_HOME'] ='/usr/lib/spark' 

os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8' 

 

dag_id ='project_7' 

 

with DAG( 

                   dag_id =  dag_id, 

                   start_date = datetime(2022, 11, 22), 

                   schedule_interval = timedelta(weeks = 1), 

                   catchup = False, 

) as dag: 

    def stage_1(): 

        import findspark 

        findspark.init() 

        findspark.find() 

        import os 

        os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf' 

        os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf' 

 

 

        from pyspark.sql import SparkSession 

        import pyspark.sql.functions as Func 

        from pyspark.sql.window import Window 

        spark = SparkSession.builder \ 

                            .master("local") \ 

                            .getOrCreate() 

 

        events = spark.read.option("basePath", "/user/master/data/geo/events").parquet("/user/master/data/geo/events/")\ 

                        .where("event.message_ts is not null")\ 

                        .select(Func.col("event.message_from").alias("user_id"), Func.col("lat").alias("lat_1"), Func.col("lon").alias("lng_1"), Func.col("event.message_ts").alias("message_ts")) 

 

        main_geo_loc = spark.read.csv(path = "/user/name/geo.csv", sep=";", header=True) 

        loc = main_geo_loc.select("id", "city", Func.regexp_replace(Func.col("lat"),',','.').cast("float").alias("lat_2"), Func.regexp_replace(Func.col("lng"),',','.').cast("float").alias("lng_2")) 

 

        joined = events.crossJoin(loc) 

        all_length = joined.select("user_id", "lat_1", "lng_1", "message_ts", "id", "city", "lat_2", "lng_2",((Func.asin(Func.sqrt(Func.pow((Func.sin(((Func.radians('lat_1') - Func.radians('lat_2'))/2))),2)+Func.cos(Func.radians('lat_1'))*Func.cos(Func.radians('lat_2'))*(Func.pow((Func.sin(((Func.radians('lng_1') - Func.radians('lng_2'))/2))),2)))))*2*6371).alias("length")) 

 

        window = Window().partitionBy(['user_id', 'message_ts']).orderBy(Func.asc('length')) 

        df_window = all_length.withColumn("rn", Func.row_number().over(window)) 

        mes_city = df_window.where('rn=1')\ 

                .select("user_id", "lat_1", "lng_1", "message_ts", "id", "city", "lat_2", "lng_2", "length") 

 

        dt = mes_city.withColumn('Data', Func.col('message_ts').cast("timestamp")) 

        dt.createOrReplaceTempView("table") 

 

        act_home_city = spark.sql(""" 

            with dt as 

                (select 

                    user_id, 

                    city home_city, 

                    Data, 

                    lag(city) over(partition by user_id order by Data) lg 

                from table 

                ) 

            ,rng as 

                (select 

                    user_id, 

                    home_city, 

                    Data, 

                    DATEDIFF(Data, coalesce((lead(Data) over(partition by user_id order by Data)), now())) cnt_day 

                from dt 

                where home_city != lg 

                ) 

            ,home_city_all as 

                ( 

                select 

                    user_id, 

                    home_city, 

                    row_number() over (partition by user_id order by Data desc) rn 

                from rng 

                where cnt_day >= 27 

                ) 

            ,home_city as 

                ( 

                select 

                    user_id, 

                    home_city 

                from home_city_all 

                where rn = 1 

                ) 

            ,a as 

                (select 

                    user_id, 

                    city act_city, 

                    row_number() over(partition by user_id order by Data desc) rn 

                from table ) 

            ,act_city as 

                (select 

                    user_id, 

                    act_city 

                from a 

                where rn = 1) 

            select 

                ac.user_id, 

                act_city, 

                home_city 

            from act_city ac left join home_city hc on ac.user_id = hc.user_id 

            """) 

 

        act_home_city.createOrReplaceTempView("table1") 

 

        table_ex_1 = spark.sql(""" 

            with timezone as 

                ( 

                select 'Sydney' city, 'Australia/Sydney' timezoon union all 

                select 'Melbourne' city, 'Australia/Melbourne' timezoon union all 

                select 'Brisbane' city, 'Australia/Brisbane' timezoon union all 

                select 'Perth' city, 'Australia/Perth' timezoon union all 

                select 'Adelaide' city, 'Australia/Adelaide' timezoon union all 

                select 'Gold Coast' city, 'Australia/Brisbane' timezoon union all 

                select 'Cranbourne' city, 'Australia/Melbourne' timezoon union all 

                select 'Canberra' city, 'Australia/Canberra' timezoon union all 

                select 'Newcastle' city, 'Australia/Sydney' timezoon union all 

                select 'Wollongong' city, 'Australia/Sydney' timezoon union all 

                select 'Geelong' city, 'Australia/Melbourne' timezoon union all 

                select 'Hobart' city, 'Australia/Hobart' timezoon union all 

                select 'Townsville' city, 'Australia/Brisbane' timezoon union all 

                select 'Ipswich' city, 'Australia/Brisbane' timezoon union all 

                select 'Cairns' city, 'Australia/Brisbane' timezoon union all 

                select 'Toowoomba' city, 'Australia/Brisbane' timezoon union all 

                select 'Darwin' city, 'Australia/Darwin' timezoon union all 

                select 'Ballarat' city, 'Australia/Melbourne' timezoon union all 

                select 'Bendigo' city, 'Australia/Melbourne' timezoon union all 

                select 'Launceston' city, 'Australia/Hobart' timezoon union all 

                select 'Mackay' city, 'Australia/Brisbane' timezoon union all 

                select 'Rockhampton' city, 'Australia/Brisbane' timezoon union all 

                select 'Maitland' city, 'Australia/Sydney' timezoon union all 

                select 'Bunbury' city, 'Australia/Perth' timezoon 

                ), 

            cnt_city as 

                ( 

                select 

                    user_id, 

                    city, 

                    cast(Data as date) Data, 

                    lead(city) over(partition by user_id order by cast(Data as date)) ld 

                from table 

                ) 

            ,travel_count as 

                ( 

                select 

                    user_id, 

                    count(*) travel_count 

                from cnt_city 

                where city!=ld 

                group by 1 

                ) 

            ,travel_array_count 

                (select 

                    cc.user_id, 

                    collect_list(city) travel_array, 

                    travel_count 

                from cnt_city cc join travel_count c on cc.user_id = c.user_id 

                where city!=ld or ld is null 

                group by 1,3 

                ), 

            max_action as 

                ( 

                select 

                    user_id, 

                    timezoon timezone, 

                    row_number() over(partition by user_id order by max(Data) desc) rn, 

                    max(Data) last_event_time 

                from table t 

                     join timezone tz on t.city = tz.city 

                group by 1,2 

                ) 

            select 

                t.*, 

                travel_array, 

                travel_count, 

                timezone, 

                from_utc_timestamp(last_event_time, timezone) local_time 

            from table1 t 

                    join travel_array_count tac on t.user_id = tac.user_id 

                    join (select * from max_action where rn = 1) ma on ma.user_id = t.user_id 

            order by t.user_id 

                """) 

 

        table_ex_1.write.mode("overwrite").parquet("/user/name/table_ex_1") 

 

    def stage_2(): 

        import findspark 

        findspark.init() 

        findspark.find() 

        import os 

        os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf' 

        os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf' 

 

        from pyspark.sql import SparkSession 

        import pyspark.sql.functions as Func 

        from pyspark.sql.window import Window 

        spark = SparkSession.builder \ 

            .master("local") \ 

            .getOrCreate() 

 

        events = spark.read.option("basePath", "/user/master/data/geo/events").parquet("/user/master/data/geo/events/") \ 

            .where("event.message_ts is not null") \ 

            .select(Func.col("event.message_from").alias("user_id"), Func.col("lat").alias("lat_1"), 

                    Func.col("lon").alias("lng_1"), "event_type", Func.col("event.message_ts").alias("message_ts")) 

        events.distinct().show() 

 

        main_geo_loc = spark.read.csv(path="/user/name/geo.csv", sep=";", header=True) 

        loc = main_geo_loc.select("id", "city", 

                                  Func.regexp_replace(Func.col("lat"), ',', '.').cast("float").alias("lat_2"), 

                                  Func.regexp_replace(Func.col("lng"), ',', '.').cast("float").alias("lng_2")) 

 

        joined = events.crossJoin(loc) 

        all_length = joined.select("user_id", "lat_1", "lng_1", "message_ts", "id", "city", "event_type", "lat_2", 

                                   "lng_2", ((Func.asin(Func.sqrt( 

                Func.pow((Func.sin(((Func.radians('lat_1') - Func.radians('lat_2')) / 2))), 2) + Func.cos( 

                    Func.radians('lat_1')) * Func.cos(Func.radians('lat_2')) * ( 

                    Func.pow((Func.sin(((Func.radians('lng_1') - Func.radians('lng_2')) / 2))), 

                             2))))) * 2 * 6371).alias("length")) 

 

        window = Window().partitionBy(['user_id', 'message_ts']).orderBy(Func.asc('length')) 

        df_window = all_length.withColumn("rn", Func.row_number().over(window)) 

        mes_city = df_window.where('rn=1') \ 

            .select("user_id", "lat_1", "lng_1", "message_ts", "id", "city", "event_type", "lat_2", "lng_2", "length") 

 

        dt = mes_city.withColumn('Data', Func.substring('message_ts', 1, 10).cast("date")) 

        dt.createOrReplaceTempView("table") 

 

        table_ex_2 = spark.sql(""" 

            with wh as 

                ( 

                select 

                    date_trunc('month', Data) month, 

                    date_trunc('week', Data) week, 

                    id zone_id, 

                    sum(case when event_type = 'message' then 1 else 0 end) week_message, 

                    sum(case when event_type = 'reaction' then 1 else 0 end) week_reaction, 

                    sum(case when event_type = 'subscription' then 1 else 0 end) week_subscription 

                from table 

                group by 1,2,3 

                ), 

            mnth as 

                ( 

                select 

                    date_trunc('month', Data) month, 

                    id zone_id, 

                    sum(case when event_type = 'message' then 1 else 0 end) month_message, 

                    sum(case when event_type = 'reaction' then 1 else 0 end) month_reaction, 

                    sum(case when event_type = 'subscription' then 1 else 0 end) month_subscription 

                from table 

                group by 1,2 

                ), 

            min_dt as 

                ( 

                select 

                    id, 

                    min(Data) m_data 

                from table 

                where event_type = 'message' 

                group by 1 

                ), 

            reg_week as 

                ( 

                select 

                    date_trunc('month', m_data) month, 

                    date_trunc('week', m_data) week, 

                    id zone_id, 

                    count(*) week_user 

                from min_dt 

                group by 1,2,3 

                ), 

            reg_month as 

                ( 

                select 

                    date_trunc('month', m_data) month, 

                    id zone_id, 

                    count(*) month_user 

                from min_dt 

                group by 1,2 

                ) 

            select 

                wh.month, 

                wh.week, 

                wh.zone_id, 

                week_message, 

                week_reaction, 

                week_subscription, 

                week_user, 

                month_message, 

                month_reaction, 

                month_subscription, 

                month_user 

            from wh 

                 join mnth on wh.zone_id = mnth.zone_id and wh.month = mnth.month 

                 join reg_week on wh.zone_id = reg_week.zone_id and wh.week = reg_week.week 

                 join reg_month on wh.zone_id = reg_month.zone_id and wh.month = reg_month.month 

                """) 

 

        table_ex_2.write.mode("overwrite").parquet("/user/name/table_ex_2") 

 

    def stage_3(): 

        import findspark 

        findspark.init() 

        findspark.find() 

        import os 

        os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf' 

        os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf' 

 

        from pyspark.sql import SparkSession 

        import pyspark.sql.functions as Func 

        from pyspark.sql.window import Window 

        spark = SparkSession.builder \ 

            .master("local") \ 

            .getOrCreate() 

 

        events = spark.read.option("basePath", "/user/master/data/geo/events").parquet("/user/master/data/geo/events/") \ 

            .where("event.subscription_channel is not null") \ 

            .select(Func.col("lat").alias("lat_1"), Func.col("lon").alias("lng_1"), "event.subscription_channel", 

                    Func.col("event.user").alias("subscription_user"), 

                    Func.col("event.datetime").alias("subscription_ts")) 

 

        events.dtypes 

 

        main_geo_loc = spark.read.csv(path="/user/name/geo.csv", sep=";", header=True) 

        loc = main_geo_loc.select("id", "city", 

                                    Func.regexp_replace(Func.col("lat"), ',', '.').cast("float").alias("lat_2"), 

                                    Func.regexp_replace(Func.col("lng"), ',', '.').cast("float").alias("lng_2")) 

 

        joined = events.crossJoin(loc) 

        all_length = joined.select("subscription_ts", "subscription_channel", "subscription_user", "lat_1", "lng_1", 

                                    "id", "city", "lat_2", "lng_2", ((Func.asin(Func.sqrt( 

                Func.pow((Func.sin(((Func.radians('lat_1') - Func.radians('lat_2')) / 2))), 2) + Func.cos( 

                    Func.radians('lat_1')) * Func.cos(Func.radians('lat_2')) * ( 

                    Func.pow((Func.sin(((Func.radians('lng_1') - Func.radians('lng_2')) / 2))), 

                                2))))) * 2 * 6371).alias("length")) 

 

        window = Window().partitionBy(['subscription_user', 'subscription_ts']).orderBy(Func.asc('length')) 

        df_window = all_length.withColumn("rn", Func.row_number().over(window)) 

        mes_city = df_window.where('rn=1') \ 

            .select("subscription_ts", "subscription_channel", "subscription_user", "lat_1", "lng_1", "id", "city", 

                    "lat_2", "lng_2", "length") 

 

        events1 = spark.read.option("basePath", "/user/master/data/geo/events").parquet("/user/master/data/geo/events/") \ 

            .where("event.message_ts is not null") \ 

            .select("event.message_from", "event.message_to") 

 

        dt = mes_city.withColumn('Data', Func.col('subscription_ts').cast("timestamp")) 

        dt.createOrReplaceTempView("table") 

        events1.createOrReplaceTempView("table1") 

 

        frt = spark.sql(""" 

                with timezone as 

                    ( 

                    select 'Sydney' city, 'Australia/Sydney' timezoon union all 

                    select 'Melbourne' city, 'Australia/Melbourne' timezoon union all 

                    select 'Brisbane' city, 'Australia/Brisbane' timezoon union all 

                    select 'Perth' city, 'Australia/Perth' timezoon union all 

                    select 'Adelaide' city, 'Australia/Adelaide' timezoon union all 

                    select 'Gold Coast' city, 'Australia/Brisbane' timezoon union all 

                    select 'Cranbourne' city, 'Australia/Melbourne' timezoon union all 

                    select 'Canberra' city, 'Australia/Canberra' timezoon union all 

                    select 'Newcastle' city, 'Australia/Sydney' timezoon union all 

                    select 'Wollongong' city, 'Australia/Sydney' timezoon union all 

                    select 'Geelong' city, 'Australia/Melbourne' timezoon union all 

                    select 'Hobart' city, 'Australia/Hobart' timezoon union all 

                    select 'Townsville' city, 'Australia/Brisbane' timezoon union all 

                    select 'Ipswich' city, 'Australia/Brisbane' timezoon union all 

                    select 'Cairns' city, 'Australia/Brisbane' timezoon union all 

                    select 'Toowoomba' city, 'Australia/Brisbane' timezoon union all 

                    select 'Darwin' city, 'Australia/Darwin' timezoon union all 

                    select 'Ballarat' city, 'Australia/Melbourne' timezoon union all 

                    select 'Bendigo' city, 'Australia/Melbourne' timezoon union all 

                    select 'Launceston' city, 'Australia/Hobart' timezoon union all 

                    select 'Mackay' city, 'Australia/Brisbane' timezoon union all 

                    select 'Rockhampton' city, 'Australia/Brisbane' timezoon union all 

                    select 'Maitland' city, 'Australia/Sydney' timezoon union all 

                    select 'Bunbury' city, 'Australia/Perth' timezoon 

                    ), 

                lat_lng as 

                    ( 

                    select 

                        subscription_user, 

                        lat_1, 

                        lng_1, 

                        row_number() over(partition by subscription_user order by Data desc) rn 

                    from table 

                    ), 

                all_users as 

                    ( 

                    select distinct 

                        t1.subscription_user user_left, 

                        t2.subscription_user user_right, 

                        now() processed_dttm, 

                        t1.id zone_id, 

                        t1.city 

                    from table t1 join table t2 on t1.subscription_channel = t2.subscription_channel 

                    where t1.subscription_user != t2.subscription_user 

                    ), 

                rn as 

                    ( 

                    select *, 

                        CAST(user_left as string) + cast(user_right as string) users, 

                        row_number() over(partition by case when cast(CAST(user_left as string) || cast(user_right as string) as int) < cast(CAST(user_right as string) || cast(user_left as string)as int) 

                                                            then cast(CAST(user_left as string) || cast(user_right as string) as int) 

                                                            else cast(CAST(user_right as string) || cast(user_left as string)as int) 

                                                            end order by user_left) rn 

                    from all_users 

                    ) 

                select 

                    user_left, 

                    user_right, 

                    processed_dttm, 

                    zone_id, 

                    from_utc_timestamp(processed_dttm, timezoon) local_time, 

                    t.lat_1, 

                    t.lng_1, 

                    t1.lat_1 lat_2, 

                    t1.lng_1 lng_2 

                from rn 

                        join (select 

                                subscription_user, 

                                lat_1, 

                                lng_1 

                            from lat_lng where rn = 1) t on t.subscription_user = rn.user_left 

                        join (select 

                                subscription_user, 

                                lat_1, 

                                lng_1 

                            from lat_lng where rn = 1) t1 on t1.subscription_user = rn.user_right 

                    join timezone tz on tz.city=rn.city 

                    left join table1 on rn.user_left=table1.message_from and rn.user_right=table1.message_to 

                    left join table1 t2 on rn.user_right=t2.message_from and rn.user_left=t2.message_to 

                where rn=1 

                    and table1.message_from is null 

                    and table1.message_to is null 

                    and t2.message_from is null 

                    and t2.message_to is null 

                    """) 

 

        length_for_clients = frt.select("user_left", "user_right", "processed_dttm", "zone_id", "local_time", "lat_1", "lng_1", "lat_2", "lng_2", ((Func.asin(Func.sqrt( 

                Func.pow((Func.sin(((Func.radians('lat_1') - Func.radians('lat_2')) / 2))), 2) + Func.cos( 

                    Func.radians('lat_1')) * Func.cos(Func.radians('lat_2')) * ( 

                    Func.pow((Func.sin(((Func.radians('lng_1') - Func.radians('lng_2')) / 2))), 

                                2))))) * 2 * 6371).alias("length")) 

 

 

        length_for_clients = length_for_clients.where("length<1") \ 

            .select('user_left', 'user_right', 'processed_dttm', 'zone_id', 'local_time') 

 

        length_for_clients.write.mode("overwrite").parquet("/user/name/length_for_clients") 

stage_1 = PythonOperator( 

            task_id = 'stage_1', 

            python_callable = stage_1(), 

            dag=dag) 

stage_2 = PythonOperator( 

            task_id = 'stage_2', 

            python_callable = stage_2(), 

            dag=dag) 

stage_3 = PythonOperator( 

            task_id = 'stage_3', 

            python_callable = stage_3(), 

            dag=dag) 

 

stage_1>>stage_2>>stage_3
