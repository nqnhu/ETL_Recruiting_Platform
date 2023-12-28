# Import Library
import os
import time
import datetime
import pyspark.sql.functions as sf
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import lit, col
import time_uuid 
from uuid import *
from uuid import UUID
from pyspark.sql import Row
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id

# Create a SparkSession to connect to Cassandra and MySQL Server
spark = SparkSession.builder \
    .appName("SparkCassandraMySQLExample") \
    .config("spark.cassandra.connection.host", "localhost") \
    .config("spark.cassandra.connection.port", "9042") \
    .config("spark.jars.packages", "com.datastax.spark:spark-cassandra-connector_2.12:3.2.0,mysql:mysql-connector-java:8.0.23") \
    .getOrCreate()

# Convert time uuid to date time
def process_timeuuid(df):
    uuidtime_list = []
    datetime_list = []
    create_time_list = df.select('create_time').collect()
    for i in range(len(create_time_list)):
        uuidtime_list.append(create_time_list[i][0])
        date_time = time_uuid.TimeUUID(bytes=UUID(create_time_list[i][0]).bytes).get_datetime().strftime("%Y-%m-%d %H:%M:%S")
        datetime_list.append(date_time)
    time_data = spark.createDataFrame(zip(uuidtime_list, datetime_list), ['create_time', 'ts'])
    result = df.drop(df.ts).join(time_data, 'create_time', 'inner')
    result = result.select('create_time','ts','job_id','custom_track','bid','campaign_id','group_id','publisher_id')
    return result

def calculating_clicks(df):
    clicks_data = df.filter(df.custom_track == 'click')
    clicks_data = clicks_data.na.fill({'bid':0})
    clicks_data = clicks_data.na.fill({'job_id':0})
    clicks_data = clicks_data.na.fill({'publisher_id':0})
    clicks_data = clicks_data.na.fill({'group_id':0})
    clicks_data = clicks_data.na.fill({'campaign_id':0})
    clicks_data.registerTempTable('clicks')
    clicks_output = spark.sql("""select job_id, date(ts) as date, hour(ts) as hour, publisher_id, campaign_id, group_id, avg(bid) as bid_set, count(*) as clicks, sum(bid) as spend_hour
                                 from clicks group by job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id""")
    return clicks_output 

def calculating_conversion(df):
    conversion_data = df.filter(df.custom_track == 'conversion')
    conversion_data = conversion_data.na.fill({'job_id':0})
    conversion_data = conversion_data.na.fill({'publisher_id':0})
    conversion_data = conversion_data.na.fill({'group_id':0})
    conversion_data = conversion_data.na.fill({'campaign_id':0})
    conversion_data.registerTempTable('conversion')
    conversion_output = spark.sql("""select job_id, date(ts) as date, hour(ts) as hour, publisher_id, campaign_id, group_id, count(*) as conversions
                                     from conversion group by job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id""")
    return conversion_output 

def calculating_qualified(df):    
    qualified_data = df.filter(df.custom_track == 'qualified')
    qualified_data = qualified_data.na.fill({'job_id':0})
    qualified_data = qualified_data.na.fill({'publisher_id':0})
    qualified_data = qualified_data.na.fill({'group_id':0})
    qualified_data = qualified_data.na.fill({'campaign_id':0})
    qualified_data.registerTempTable('qualified')
    qualified_output = spark.sql("""select job_id, date(ts) as date, hour(ts) as hour, publisher_id, campaign_id, group_id, count(*) as qualified
                                    from qualified group by job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id""")
    return qualified_output

def calculating_unqualified(df):
    unqualified_data = df.filter(df.custom_track == 'unqualified')
    unqualified_data = unqualified_data.na.fill({'job_id':0})
    unqualified_data = unqualified_data.na.fill({'publisher_id':0})
    unqualified_data = unqualified_data.na.fill({'group_id':0})
    unqualified_data = unqualified_data.na.fill({'campaign_id':0})
    unqualified_data.registerTempTable('unqualified')
    unqualified_output = spark.sql("""select job_id, date(ts) as date, hour(ts) as hour, publisher_id, campaign_id, group_id, count(*) as unqualified
                                      from unqualified group by job_id, date(ts), hour(ts), publisher_id, campaign_id, group_id""")
    return unqualified_output

def process_final_data(clicks_output, conversion_output, qualified_output, unqualified_output):
    final_data = clicks_output.join(conversion_output, ['job_id', 'date', 'hour', 'publisher_id', 'campaign_id', 'group_id'], 'full') \
                              .join(qualified_output, ['job_id', 'date', 'hour', 'publisher_id', 'campaign_id', 'group_id'], 'full') \
                              .join(unqualified_output, ['job_id', 'date', 'hour', 'publisher_id', 'campaign_id', 'group_id'], 'full')
    return final_data 

def process_cassandra_data(df):
    clicks_output = calculating_clicks(df)
    conversion_output = calculating_conversion(df)
    qualified_output = calculating_qualified(df)
    unqualified_output = calculating_unqualified(df)
    final_data = process_final_data(clicks_output, conversion_output, qualified_output, unqualified_output)
    return final_data

def retrieve_company_data(url, driver, user, password):
    sql = """(SELECT id as job_id, company_id, group_id, campaign_id FROM job) test"""
    company = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load()
    return company 

def import_to_mysql(output, url, driver, user, password):
    final_output = output.select('job_id', 'date', 'hour', 'publisher_id', 'company_id', 'campaign_id', 'group_id', 'unqualified', 'qualified', 'conversions', 'clicks', 'bid_set', 'spend_hour')
    final_output = final_output.withColumnRenamed('date', 'dates').withColumnRenamed('hour', 'hours').withColumnRenamed('qualified', 'qualified_application') \
                               .withColumnRenamed('unqualified', 'disqualified_application').withColumnRenamed('conversions', 'conversion')
    final_output = final_output.withColumn('sources', lit('Cassandra'))
    final_output.show()
    final_output.write.format("jdbc") \
                      .option(url=url, driver=driver, user=user, password=password) \
                      .option("dbtable", "events") \
                      .mode("append") \
                      .save()
    return print('Data imported successfully')

def main_task(mysql_time):
    host = 'localhost'
    port = '3306'
    db_name = 'SDE_DataWarehouse'
    user = 'root'
    password = '1'
    url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
    driver = "com.mysql.cj.jdbc.Driver"

    print("-----------------------------")
    print("Retrieve data from Cassandra")
    print("-----------------------------")
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tracking", keyspace="Study_Data_Engineering").load().where(col('ts') >= mysql_time)
    df.show()

    print("-----------------------------")
    print("Convert UUID to datetime")
    print("-----------------------------")
    process_df = process_timeuuid(df)

    print("-----------------------------")
    print("Process Cassandra statistics")
    print("-----------------------------")
    cassandra_output = process_cassandra_data(process_df)

    print("-----------------------------")
    print("Retrieving Company ID")
    print("-----------------------------")
    company = retrieve_company_data(url, driver, user, password)

    print("-----------------------------")
    print("Finalizing Output")
    print("-----------------------------")
    final_output = cassandra_output.join(company, 'job_id', 'left').drop(company.group_id).drop(company.campaign_id)
    
    print("-----------------------------")
    print("Import data to MySQL")
    print("-----------------------------")
    import_to_mysql(final_output)

    return print('Task Finished')

# Data Streaming with CDC (Change Data Capture)
def retrieve_cassandra_latest_time():
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table="tracking", keyspace="Study_Data_Engineering").load()
    cassandra_time = df.agg({'ts':'max'}).take(1)[0][0]
    return cassandra_time 

def retrieve_latest_time_mysql(url, driver, user, password):
    sql = """(select max(last_updated_time) from events) test"""
    mysql_time = spark.read.format('jdbc').options(url=url, driver=driver, dbtable=sql, user=user, password=password).load().take(1)[0][0]
    if mysql_time is None:
        mysql_latest = '1998-01-01 23:59:59'
    else :
        mysql_latest = mysql_time.strftime('%Y-%m-%d %H:%M:%S')
    return mysql_latest

while True:
    host = 'localhost'
    port = '3306'
    db_name = 'SDE_DataWarehouse'
    user = 'root'
    password = '1'
    url = 'jdbc:mysql://' + host + ':' + port + '/' + db_name
    driver = "com.mysql.cj.jdbc.Driver"

    timestart = datetime.datetime.now()
    print("It is {} at the moment".format(timestart))

    mysql_time = retrieve_latest_time_mysql(url,driver,user,password)
    print('Latest time in MySQL is {}'.format(mysql_time))
    
    cassandra_time = retrieve_cassandra_latest_time()
    print('Latest time in Cassandra is {}'.format(cassandra_time))
    
    if cassandra_time > mysql_time :
        main_task(mysql_time)
    else :
        print('No New data found')
    timeend = datetime.datetime.now()
    print("It is {} at the moment".format(timeend))
    print("Job takes {} seconds to execute".format((timeend - timestart).total_seconds()))
    print("-------------------------------------------")
    time.sleep(30)