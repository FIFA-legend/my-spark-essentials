package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, countDistinct, expr, max}

object Joins extends App {

  /*
    Join - wide transformation
   */

  val spark = SparkSession.builder()
    .appName("Joins")
    .config("spark.master", "local")
    .getOrCreate()

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars.json")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers.json")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands.json")

  // joins
  // inner - все, что удовлетворяет условию, входит, остальное - отбрасывается
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  // outer joins
  // left outer = everything in inner join + all the rows in the LEFT table, with nulls
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")

  // right outer = everything in inner join + all the rows in the RIGHT table, with nulls
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")

  // outer join = everything in the inner join + all the rows in BOTH tables, with nulls
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi-joins = everything in the left DF for which there is a row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti-join = everything in the left DF for which there is NO row in the right DF satisfying the condition
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")

  // thing to bear in mind

  // option 1 - rename the column on which we are joining
  guitaristsDF.join(bandsDF.withColumnRenamed("id", "band"), "band")

  // option 2 - drop the dupe column
  guitaristsBandsDF.drop(bandsDF.col("id"))

  // spark присваивает столбцам УНИКАЛЬНЫЕ id

  // option 3 - rename the offending column and keep the data
  val bandsModDF = bandsDF.withColumnRenamed("id", "bandId")
  guitaristsDF.join(bandsModDF, guitaristsDF.col("band") === bandsModDF.col("bandId"))

  // using complex types
  guitaristsDF.join(guitarsDF.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"))

  /**
    * Exercises
    *
    * 1. Show all employees and their max salary
    * 2. Show all employees who were never a manager
    * 3. Find the job titles of the best paid 10 employees in the company
    */

  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.employees")
    .load()

  val salariesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.salaries")
    .load()

  val managersDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.dept_manager")
    .load()

  val titlesDF = spark.read
    .format("jdbc")
    .option("driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/rtjvm")
    .option("user", "docker")
    .option("password", "docker")
    .option("dbtable", "public.titles")
    .load()

  // 1
  val maxSalaryDF = salariesDF
    .groupBy(col("emp_no"))
    .agg(
      max(col("salary")).as("salary")
    )

  employeesDF.join(maxSalaryDF, "emp_no").show()

  // 2
  employeesDF.join(managersDF, employeesDF.col("emp_no") === managersDF.col("emp_no"), "left_anti").show()

  // 3
  val lastTitlesDF = titlesDF
    .groupBy(col("emp_no"), col("title"))
    .agg(
      max("from_date")
    )

  employeesDF.join(maxSalaryDF, "emp_no")
    .orderBy(col("salary").desc)
    .limit(10)
    .join(lastTitlesDF, "emp_no")
    .orderBy(col("salary").desc)
    .show()

  maxSalaryDF.orderBy(col("salary").desc).show()

}
