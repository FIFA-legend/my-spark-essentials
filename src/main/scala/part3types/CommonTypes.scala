package part3types

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("Common spark types")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // adding a plain value to a DF
  moviesDF.select(col("Title"), lit(47).as("plain_value"))

  // Booleans
  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredFilter = dramaFilter and goodRatingFilter

  moviesDF.select("Title").where(dramaFilter and goodRatingFilter)

  val moviesWithGoodnessFlagsDF = moviesDF.select(col("Title"), preferredFilter.as("good_movie"))
  moviesWithGoodnessFlagsDF.where("good_movie")

  // negations
  moviesWithGoodnessFlagsDF.where(not(col("good_movie")))

  // Numbers

  // math operators
  val moviesAverageRatingDF = moviesDF.select(col("Title"), (col("Rotten_Tomatoes_Rating") / 2 + col("IMDB_Rating")) / 2)

  // correlation = number between -1 and 1
  // corr is an ACTION
  println(moviesDF.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  // Strings
  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // initcap, lower, upper
  carsDF.select(initcap(col("Name"))).show()

  // contains
  carsDF.select("*").where(col("Name").contains("volkswagen"))

  // regex
  val regexString = "volkswagen|vw"
  val vwDF = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), regexString, 0).as("regex_extract")
  ).where(col("regex_extract") =!= "")

  vwDF.select(
    col("Name"),
    regexp_replace(col("Name"), regexString, "People's Car").as("regex_replace")
  )

  /**
    * Exercise
    *
    * 1. Filter the cars DF by a list of car names obtained by an API call
    * Versions:
    * - contains
    * - regexes
    */

  val list = List("volkswagen", "vw")

  // contains
  val carNameFilters = list.map(_.toLowerCase).map(name => col("Name").contains(name))
  val filter = carNameFilters.fold(lit(false))((filters, newFilter) => filters or newFilter)

  val listCarsDF = carsDF.filter(filter)

  //regex
  val listRegexString = list.mkString("|")
  val listCarsDF2 = carsDF.select(
    col("Name"),
    regexp_extract(col("Name"), listRegexString, 0).as("regex")
  ).where(col("regex") =!= "")

}
