package part3types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Data Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val moviesWithReleaseDates = moviesDF.select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release"))
    .withColumn("Today", current_date()) // today
    .withColumn("Right_now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_Release")) / 365) // date_add, date_sub

  /**
    * Exercise
    * 1. How to we deal with multiple formats?
    * 2. Read the stocks DF and parse dates
    */

    // 1 - parse the DF multiple times then union the small DFs

  // 2
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.select(col("symbol"), to_date(col("date"), "MMM dd YYYY").as("Date")).show()

}
