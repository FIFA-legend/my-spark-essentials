package part7bigdata

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._

object TaxiApplication extends App {

  val spark = SparkSession.builder()
    .appName("Taxi Big Data")
    .config("spark.master", "local")
    .getOrCreate()
  import spark.implicits._

  val taxiDF = spark.read.load("src/main/resources/data/yellow_taxi_jan_25_2018")
  taxiDF.printSchema()
  println(taxiDF.count())

  val taxiZonesDF = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("src/main/resources/data/taxi_zones.csv")
  taxiZonesDF.printSchema()

  // 1
  val pickupsByTaxiZoneDF = taxiDF.groupBy("PULocationID")
    .agg(count("*").as("totalTrips"))
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("totalTrips").desc_nulls_last)

  // 1b - group by borough
  val pickupsByBorough = pickupsByTaxiZoneDF.groupBy(col("Borough"))
    .agg(sum(col("totalTrips")).as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  // 2
  val pickupsByHourDF = taxiDF.withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy(col("hour_of_day"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  // 3
  val tripDistanceDF = taxiDF.select(col("trip_distance").as("distance"))
  val longDistanceThreshold = 30
  val tripDistanceStatsDF = tripDistanceDF.select(
    count("*").as("count"),
    lit(longDistanceThreshold).as("threshold"),
    mean("distance").as("mean"),
    stddev("distance").as("stddev"),
    min("distance").as("min"),
    max("distance").as("max"),
  )

  val tripsWithLengthDF = taxiDF.withColumn("isLong", col("trip_distance") >= longDistanceThreshold)
  val tripsByLengthDF = tripsWithLengthDF.groupBy(col("isLong")).count()

  // 4
  val pickupsByHourByLengthDF = tripsWithLengthDF.withColumn("hour_of_day", hour(col("tpep_pickup_datetime")))
    .groupBy(col("hour_of_day"), col("isLong"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  // 5
  def pickupDropOffPopularity(predicate: Column) = {
    tripsWithLengthDF
      .where(predicate)
      .groupBy("PULocationID", "DOLocationID").agg(count("*").as("totalTrips"))
      .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
      .withColumnRenamed("Zone", "Pickup_Zone")
      .drop("LocationID", "service_zone", "Borough")
      .join(taxiZonesDF, col("DOLocationID") === col("LocationID"))
      .withColumnRenamed("Zone", "DropOof_Zone")
      .drop("LocationID", "service_zone", "Borough")
      .drop("PULocationID", "DOLocationID")
      .orderBy(col("totalTrips").desc_nulls_last)
  }

  // 6
  val rateCodeDistributionDF = taxiDF
    .groupBy(col("RatecodeID")).agg(count("*").as("totalTrips"))
    .orderBy(col("totalTrips").desc_nulls_last)

  // 7
  val rateCodeEvolutionDF = taxiDF
    .groupBy(to_date(col("tpep_pickup_datetime")).as("pickup_day"), col("RatecodeID"))
    .agg(count("*").as("totalTrips"))
    .orderBy(col("pickup_day"), col("RatecodeID"))

  // 8
  val groupAttemptsDF = taxiDF
    .select(
      round(unix_timestamp(col("tpep_pickup_datetime")) / 300).cast("integer").as("fiveMinId"),
      col("PULocationID"),
      col("total_amount")
    )
    .where(col("passenger_count") < 3)
    .groupBy(col("fiveMinId"), col("PULocationID"))
    .agg(count("*").as("total_trips"), sum(col("total_amount")).as("total_amount"))
    .withColumn("approximate_datetime", from_unixtime(col("fiveMinId") * 300))
    .drop("fiveMinId")
    .join(taxiZonesDF, col("PULocationID") === col("LocationID"))
    .drop("LocationID", "service_zone")
    .orderBy(col("total_trips").desc_nulls_last)

  val percentGroupAttempt = 0.05
  val percentAcceptGrouping = 0.3
  val discount = 5
  val extraCost = 2
  val avgCostReduction = 0.6 * taxiDF.select(avg(col("total_amount"))).as[Double].take(1)(0)

  val groupingEstimateEconomicImpactDF = groupAttemptsDF
    .withColumn("groupedRides", col("total_trips") * percentGroupAttempt)
    .withColumn("acceptedGroupedRidesEconomicImpact", col("groupedRides") * percentAcceptGrouping * (avgCostReduction - 5))
    .withColumn("rejectedGroupedRidesEconomicImpact", col("groupedRides") * (1 - percentAcceptGrouping) * extraCost)
    .withColumn("totalImpact", col("acceptedGroupedRidesEconomicImpact") + col("rejectedGroupedRidesEconomicImpact"))

  val totalProfitDF = groupingEstimateEconomicImpactDF.select(sum(col("totalImpact")).as("total"))
  // 40k/day = 12 million/year!!!
  totalProfitDF.show()

}
