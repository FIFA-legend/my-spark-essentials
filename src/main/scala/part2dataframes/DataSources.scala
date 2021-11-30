package part2dataframes

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import part2dataframes.DataFramesBasics.{cars, spark}

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Sources and Formats")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )

  /*
  Reading a DF:
    - format
    - schema or inferSchema = true
    - zero or more options
    - path
  */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema) // enforce a schema
    .option("mode", "failFast") // dropMalformed, permissive (default). Describes mode on malformed data
    .option("path", "src/main/resources/data/cars.json")
    .load() // file on computer or Amazon

  // alternative with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "inferSchema" -> "true",
      "path" -> "src/main/resources/data/cars.json"
    ))
    .load()

  /*
  Writing DF
  - format
  - save mode = overwrite, append, ignore, errorIfExists
  - path
  - zero or more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_dupe.json")
    .save()

  // JSON flags
  spark.read
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM-dd") // couple with schema; if spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  // CSV
  val stocksSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    )
  )

  spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true") // ignore first row (header)
    .option("sep", ",") // column separator
    .option("nullValue", "")
    .csv("src/main/resources/data/cars.json")

  // Parquet
  carsDF.write
    .mode(SaveMode.Overwrite)
    .parquet("src/main/resources/data/cars.parquet")

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from remote DB
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /**
    * Exercise: read the movies DF, then write it as
    * - tab-separated values file
    * - snappy Parquet
    * - table public.movies in the Postgres DB
    */

  val moviesSchema = StructType(
    Array(
      StructField("Title", StringType),
      StructField("US_Gross", IntegerType),
      StructField("Worldwide_Gross", IntegerType),
      StructField("US_DVD_Sales", IntegerType),
      StructField("Production_Budget", IntegerType),
      StructField("Release_Date", DateType),
      StructField("MPAA_Rating", StringType),
      StructField("Running_Time_min", IntegerType),
      StructField("Distributor", StringType),
      StructField("Source", StringType),
      StructField("Major_Genre", StringType),
      StructField("Creative_Type", StringType),
      StructField("Director", StringType),
      StructField("Rotten_Tomatoes_Rating", IntegerType),
      StructField("IMDB_Rating", DoubleType),
      StructField("IMDB_Votes", IntegerType)
    )
  )

  val moviesDF = spark.read
    .schema(moviesSchema)
    .option("dateFormat", "d-MMM-yy")
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed")
    //.option("mode", "failFast")
    .json("src/main/resources/data/movies.json")

  // write to TSV
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("header", "true")
    .option("sep", "\t")
    .option("nullValue", "")
    .csv("src/main/resources/data/movies.csv")

  // write to Parquet
  moviesDF.write
    .mode(SaveMode.Overwrite)
    .option("compression", "snappy")
    .parquet("src/main/resources/data/movies.parquet")

  // write to DB
  moviesDF.write
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.movies")
    .save()
}
