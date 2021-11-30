package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name")

  // Selecting (projecting)
  val carsNameDF = carsDF.select(firstColumn)

  // various select methods

  import spark.implicits._

  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column object
    expr("Origin") // EXPRESSION
  )

  // select with plain column names
  carsDF.select("Name", "Year")

  // При select выбираются соответствующие столбцы из DF, на каждом узле происходят соответствующие изменения
  // (narrow-transformation). То есть каждый комп кластера изменяет только свои файлы

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg_2")
  )

  // selectExpr
  val carsWithSelectExprWeightDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  //adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")
  //careful with column names
  carsWithColumnRenamed.select("`Weight in pounds`")
  // remove a column
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA") // for equality ===

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfulCars2DF = carsDF.filter((col("Origin") === "USA").and(col("Horsepower") > 150))
  val americanPowerfulCars3DF = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/more_cars.json")

  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allCountriesDF = carsDF.select("Origin").distinct()
  allCountriesDF.show()

  carsWithWeightsDF.show()

  /**
    * Exercises:
    * 1. Read the movies DF and select 2 columns
    * 2. Create another column summing up the total profit of the movie = US_Gross + Worldwide_Gross + US_DVD_Sales
    * 3. Select all COMEDY with IMDB rating above 6
    *
    * Use as many versions as possible
    */

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.select("Title", "US_Gross").show()
  moviesDF.select(col("Title"), col("US_Gross")).show()

  // 2
  val totalGrossColumn = moviesDF.col("US_Gross") +
    moviesDF.col("Worldwide_Gross") +
    moviesDF.col("US_DVD_Sales")

  moviesDF.withColumn("Sum_gross", totalGrossColumn)
    .show()

  moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("US_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total")
    )
    .show()


  // 3
  moviesDF.filter(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6).show()
  moviesDF.filter(col("Major_Genre") === "Comedy")
    .filter(col("IMDB_Rating") > 6)
    .show()
  moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6").show()
}
