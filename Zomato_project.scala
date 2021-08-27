package ZomatoProject

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.mean
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}

object Zomato_project {
  // case class ZomatoData(restaurant_id: Int, r_name: String, country_code: Int, city: String, address: String, locality: String, locality_verbose: String, longitude: String, latitude: String, cuisines: String, average_cost_for_two_currency: Int, has_table_booking: String, has_online_delivery: String, is_delivering_now: String, switch_to_order_menu: String, price_range: Int, aggregate_rating: Float, rating_color: String, rating_text: String, votes: Int)

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().appName("test").master("local[*]")
      .getOrCreate()
      val schema = StructType(
        List(
          StructField("restaurant_id", IntegerType, nullable = true),
          StructField("r_name", StringType, nullable = true),
          StructField("country_code", IntegerType, nullable = true),
          StructField("city", StringType, nullable = true),
          StructField("address", StringType, nullable = true),
          StructField("locality", StringType, nullable = true),
          StructField("locality_verbose", StringType, nullable = true),
          StructField("longitude", StringType, nullable = true),
          StructField("latitude", StringType, nullable = true),
          StructField("cuisines", StringType, nullable = true),
          StructField("average_cost_for_two", FloatType, nullable = true),
          StructField("currency", StringType, nullable = true),
          StructField("has_table_booking", StringType, nullable = true),
          StructField("has_online_delivery", StringType, nullable = true),
          StructField("is_delivering_now", StringType, nullable = true),
          StructField("switch_to_order_menu", StringType, nullable = true),
          StructField("price_range", IntegerType, nullable = true),
          StructField("aggregate_rating", FloatType, nullable = true),
          StructField("rating_color", StringType, nullable = true),
          StructField("rating_text", StringType, nullable = true),
          StructField("votes", IntegerType, nullable = true)
        ))
      val schemaData = spark.read.option("header", "true")
        .schema(schema)
        .csv("C:\\Users\\tniho\\Zomato_Project\\zomato.csv")
        .toDF()
      schemaData.printSchema()

      schemaData.show(2)
      schemaData.createOrReplaceTempView("zomato")


    println("""
        |*****************************************************************|
        |**   WHICH LOCATION(COUNTRY) HAS THE MOST BUYING POWER    **     |
        |*****************************************************************|
        |""".stripMargin)

      val result = schemaData.groupBy("currency").agg(mean("average_cost_for_two").alias("average_cost_for_two"))
      result.na.drop()
      result.show(5)


      println("*****************************************************************")
      println("**   LOCALITY WITH BEST RATING(EXCELLENT) AND VOTES    **")
      println("*****************************************************************")

      val res2 = spark.sql("SELECT country_code,locality,aggregate_rating,votes from zomato where aggregate_rating  > 4 AND votes > 400 ORDER BY votes DESC")
      res2.na.drop()
      res2.show(4)

      println("*****************************************************************")
      println("**   WHICH CUISINE IS DELIVERABLE(IS DELIVERY YES    **")
      println("*****************************************************************")

      val res3 = spark.sql("SELECT country_code,locality,has_online_delivery,cuisines,aggregate_rating from zomato where has_online_delivery ='Yes' AND aggregate_rating > 4 ORDER BY aggregate_rating DESC")
      res3.na.drop()
      res3.show(4)

      println("*****************************************************************")
      println("**    WHICH LOCALITY HAS ONLINE DELIVERY                **")
      println("*****************************************************************")

      val res5 = spark.sql("SELECT country_code,locality,has_online_delivery,is_delivering_now,cuisines,aggregate_rating from zomato where has_online_delivery ='Yes' AND is_delivering_now = 'Yes' AND aggregate_rating > 2 ORDER BY aggregate_rating DESC")
      res5.na.drop()
      res5.show(4)

      println("*****************************************************************")
      println("**    WHICH LOCALITY HAS TABLE BOOKING WITH GOOD VOTES    **")
      println("*****************************************************************")

      val res6 = spark.sql("SELECT country_code,locality,has_table_booking,cuisines,aggregate_rating,votes from zomato where has_table_booking ='Yes' AND aggregate_rating > 2 AND votes > 500 ORDER BY votes DESC")
      res6.na.drop()
      res6.show(4)


      val prop = new java.util.Properties()
      prop.put("user", "root")
      prop.put("password", "Tanmay@121")
      val url = "jdbc:mysql://localhost:3306/zomato"

      result.write.mode(SaveMode.Overwrite).jdbc(url, "buying_power_by_country", prop)
      res2.write.mode(SaveMode.Overwrite).jdbc(url, "buying_power_by_country1", prop)
      res3.write.mode(SaveMode.Overwrite).jdbc(url, "buying_power_by_country2", prop)
      res5.write.mode(SaveMode.Overwrite).jdbc(url, "buying_power_by_country3", prop)
      res6.write.mode(SaveMode.Overwrite).jdbc(url, "buying_power_by_country4", prop)

      println("**********************************************************************")
      println("DATA STORED IN DATABASE")
      println("**********************************************************************")

      spark.stop()
      println("**********************************************************************")
      println("** Spark Session closed safely **")
      println("**********************************************************************")
////sparkproject1_2.12-0.1.jar

  }
}
