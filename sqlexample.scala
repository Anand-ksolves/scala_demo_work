//package com.sparkbyexamples.spark

import org.apache.spark.sql.{SQLContext, SparkSession}

object sqlexample extends App {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("sqlexample")
      .getOrCreate();

//    spark.sparkContext.setLogLevel("ERROR")


//    val sqlContext: SQLContext = spark.sqlContext

    //read csv with options
    //  val df = sqlContext.read.options(Map("inferSchema"->"true","delimiter"->",","header"->"true"))
    //    .csv("src/main/scala/Pro.csv")
    //  df.show()
    //  df.printSchema()

    val schemaData = spark.read.option("header", "true").option("inferSchema", "true")
      .csv("D:\\intelliJ_Project\\src\\main\\scala\\Pro.csv")
      schemaData.printSchema()


    schemaData.createOrReplaceTempView("TAB")
//    sqlContext.sql("select * from TAB")
//      .show(false)

  }

