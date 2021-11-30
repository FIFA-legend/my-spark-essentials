package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{approx_count_distinct, avg, col, count, countDistinct, mean, min, stddev, sum}

object Aggregations extends App {

  val spark = SparkSession.builder()
    .appName("Aggregation")
    .config("spark.master", "local")
    .getOrCreate()

  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  // counting
  val genresCountDF = moviesDF.select(count(col("Major_Genre"))) // all the values except null
  moviesDF.selectExpr("count(Major_Genre)")

  // counting all
  moviesDF.select(count("*")) // count all the rows, and will INCLUDE null

  // counting distinct
  moviesDF.select(countDistinct(col("Major_genre")))

  // approximate count
  moviesDF.select(approx_count_distinct(col("Major_Genre")))

  // min and max
  val minRatingDF = moviesDF.select(min(col("IMDB_Rating")))
  val minRatingDF2 = moviesDF.selectExpr("min(IMDB_Rating)")

  // sum
  moviesDF.select(sum("US_Gross"))
  moviesDF.selectExpr("sum(US_Gross)")

  // avg
  moviesDF.select(avg(col("Rotten_Tomatoes_Rating")))
  moviesDF.selectExpr("avg(Rotten_Tomatoes_Rating)")

  // data science
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")),
    stddev(col("Rotten_Tomatoes_Rating"))
  )

  // Grouping
  val countByGenre = moviesDF
    .groupBy(col("Major_Genre")) // includes null
    .count() // select count(*) from moviesDF group by Major_Genre

  val avgRatingByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .avg("IMDB_Rating")

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))

  /**
    * Exercises
    *
    * 1. Sum up ALL the profits of ALL the movies in the DF
    * 2. Count how many distinct directors we have
    * 3. Show the mean and standard deviation of US gross revenue for the movies
    * 4. Compute the average IMDB rating and the average US gross revenue Per DIRECTOR
    */

  // 1
  moviesDF.select(
    (sum(col("US_Gross")) +
      sum(col("Worldwide_Gross")) +
      sum(col("US_DVD_Sales"))).as("Total")
  )

  // wrong result because of nulls. When summing 3 columns and one of them is null, the result is null
  moviesDF.select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total"))
    .select(sum("Total"))

  // 2
  moviesDF.select(countDistinct(col("Director")))

  // 3
  moviesDF.select(
    mean(col("US_Gross")),
    stddev(col("US_Gross"))
  )

  // 4
  moviesDF.groupBy(col("Director"))
    .agg(
      avg(col("IMDB_Rating")).as("Rating"),
      sum(col("US_Gross")).as("Gross")
    )
    .orderBy(col("Rating").desc)
    .show()

  /*
    Все трансформации из этого занятия являются широкими (Wide Transformation).
    One/more input partition => One/more output partition. Части перемещаются между компами (называется Shuffle)
    Очень затратная операция
    Лучше делать такие трансформации в конце вычислений
   */
}
