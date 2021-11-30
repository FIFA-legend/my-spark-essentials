package part3types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("Complex Types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // Dates
  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  val moviesWithReleaseDates = moviesDF.select(col("Title"), to_date(col("Release_Date"), "dd-MMM-yy").as("Actual_Release")) // conversion

  moviesWithReleaseDates
    .withColumn("Today", current_date()) // today
    .withColumn("Right_Now", current_timestamp()) // this second
    .withColumn("Movie_Age", datediff(col("Today"), col("Actual_release")) / 365) // date_add, date_sub

  moviesWithReleaseDates.select("*").where(col("Actual_Release").isNull)

  /**
    * Exercise
    * 1. How do we deal with multiple formats?
    * 2. Read the stocks DF and parse the dates
    */

  // 1 - parse the DF multiple times, then union the small DFs

  // 2
  val stocksDF = spark.read
    .option("inferSchema", "true")
    .option("header", "true")
    .option("sep", ",")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.select(col("symbol"), to_date(col("date"), "MMM dd yyyy"), col("price"))

  // Structures

  // 1 - with col operators
  moviesDF.select(col("Title"), struct("US_Gross", "Worldwide_Gross").as("Profit"))
    .select(col("Title"), col("Profit").getField("US_Gross").as("US_Profit"))

  // 2 - with expression strings
  moviesDF.selectExpr("Title", "(US_Gross, Worldwide_Gross) as Profit")
    .selectExpr("Title", "Profit.US_Gross")

  // Arrays
  val moviesWithWords = moviesDF.select(col("Title"), split(col("Title"), " |,").as("Title_Words")) // ARRAY of strings

  moviesWithWords.select(
    col("Title"),
    expr("Title_Words[0]"),
    size(col("Title_Words")),
    array_contains(col("Title_Words"), "Love")
  ).show()
}
