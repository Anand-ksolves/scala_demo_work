from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import *
from pyspark.streaming import StreamingContext
from pprint import pprint
import time
from pyspark.sql import *
from pyspark.sql.types import *

kafka_topic_name = "zomato"
kafka_bootstrap_servers = '192.168.56.1:9092'
my_sql_server='192.168.56.1:3306'
my_sql_database_name ="zomato_data"

sc=SparkContext()
ssc =StreamingContext(sc,1)

if __name__ == "__main__":
    print("Welcome to DataMaking !!!")
    print("Stream Data Processing Application Started ...")
    print(time.strftime("%Y-%m-%d %H:%M:%S"))

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    orders_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .option("startingOffsets", "latest") \
        .load()

    print("Printing Schema of orders_df: ")
    orders_df.printSchema()

    orders_df1 = orders_df.selectExpr("CAST(value AS STRING)", "timestamp")

    orders_schema_string = "name STRING, online_order STRING,  " \
                           + "book_table STRING,rate DOUBLE,votes INT, location STRING, rest_type STRING, " \
                           + "cuisines STRING,appxCostForTwo STRING"

    orders_df2 = orders_df1\
        .select(from_csv(col("value"), orders_schema_string)\
        .alias("orders"), "timestamp")

    orders_df3 = orders_df2.select("orders.*", "timestamp")
    orders_df3.printSchema()
    orders_df3.createOrReplaceTempView("table1")
    
    orders_df4 = spark.sql("select * from table1")
    print("Printing Schema of orders_df4: ")
    orders_df4.printSchema()
    
    def db_connection(orders_df4,my_sql_database_name):
        print("************************ Processing Started ************************************")
        print()
        x = spark.read.format('jdbc').options(
                    url='jdbc:mysql://host.docker.internal:3307/zomato_data',#locakhost
                    driver='com.mysql.cj.jdbc.Driver',
                    dbtable='data',
                    user='root',
                    password='root123').load()
  
        if x.count() > 0:
            print ("Db_connection Established ...")
            print()
            orders_df4.show()
            
            x1=orders_df4.join(x)
            x1.printSchema()
            x1.write.format('jdbc').options(
                        url='jdbc:mysql://host.docker.internal:3307/zomato_data',
                        driver='com.mysql.cj.jdbc.Driver',
                        dbtable='data',
                        user='root',
                        password='root123').mode('append').save()

            print()
            print ("Hurrey, Data is stored inside the database ...")
            print()
        else:
            print ("DataBase is Empty...")
            print("Adding Data...")
            orders_df4.write.format('jdbc').options(
                        url='jdbc:mysql://host.docker.internal:3307/zomato_data',
                        driver='com.mysql.cj.jdbc.Driver',
                        dbtable='data',
                        user='root',
                        password='root123').mode('append').save()
            print()
            print ("Hurrey, Data is stored inside the database ...")
            print()
            
        print("************************ Processing Ended ************************************")
        print()
    
    df_new1 = orders_df4.writeStream \
    .outputMode("append") \
    .foreachBatch(db_connection) \
    .trigger(processingTime="30 second") \
    .option("truncate", False) \
    .start() \
    .awaitTermination()
    print("Stream Data Processing Application Completed.")