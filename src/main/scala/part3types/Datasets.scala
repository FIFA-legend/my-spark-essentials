package part3types

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .config("spark.master", "local")
    .getOrCreate()

  val numbersDF: DataFrame = spark.read
    .format("csv")
    .option("inferSchema", "true")
    .option("header", "true")
    .load("src/main/resources/data/numbers.csv")

  numbersDF.printSchema()

  // convert a DF to a DS
  implicit val intEncoder = Encoders.scalaInt
  val numbersDS: Dataset[Int] = numbersDF.as[Int]

  numbersDS.filter(_ < 100)

  // dataset of a complex type
  // 1 - define your case class
  case class Car(
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Cylinders: Long,
                  Displacement: Double,
                  Horsepower: Option[Long],
                  Weight_in_lbs: Long,
                  Acceleration: Double,
                  Year: String,
                  Origin: String
                )

  // 2 - read the DF from the file
  def readDF(filename: String) = {
    spark.read
      .option("inferSchema", "true")
      .json(s"src/main/resources/data/$filename")
  }

  // 3 - define an encoder
  import spark.implicits._
  val carsDF = readDF("cars.json")
  //implicit val carEncoder = Encoders.product[Car] // to use class must extend Product (all case classes do that)

  // convert the DF to DS
  val carsDS = carsDF.as[Car]

  // DS collection functions
  numbersDS.filter(_ < 100)

  // map, flatMap, fold, reduce, for comprehensions ...
  val carNamesDS = carsDS.map(car => car.Name.toUpperCase())

  /**
    * Exercise
    *
    * 1. Count how many cars we have
    * 2. Count how many POWERFUL cars we have (HP > 140)
    * 3. Average HP for the entire DS
    */

  // 1
  val count = carsDS.count()

  // 2
  carsDS.filter(car =>
    car.Horsepower match {
      case Some(value) => value > 140
      case None => false
    }
  ).count()

  carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count()

  // 3
  carsDS.map(car =>
    car.Horsepower match {
      case Some(value) => value
      case None => 0
    }
  ).reduce(_ + _) / count.toDouble

  carsDS.select(avg(col("Horsepower")))

  // Joins
  case class Guitar(guitarId: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").withColumnRenamed("id", "guitarId").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band")  === bandsDS.col("id"))

  /**
    * Exercise: join the guitarsDS and guitarPlayersDS, in an outer join
    * (hint: use array_contains)
    */

  guitarPlayersDS.joinWith(guitarsDS, expr("array_contains(guitars, guitarId)"), "outer")
  guitarPlayersDS.joinWith(guitarsDS, array_contains(guitarPlayersDS.col("guitars"), guitarsDS.col("guitarId")), "outer")

  // Grouping DS

  val carsGroupedByOriginDS = carsDS
    .groupByKey(_.Origin)
    .count()
    .show()

  // joins and groups are WIDE transformations, will involve SHUFFLE operations

}
