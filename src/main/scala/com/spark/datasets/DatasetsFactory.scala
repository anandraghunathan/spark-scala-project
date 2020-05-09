package com.spark.datasets

import java.sql.Date

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object DatasetsFactory extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("DataSetsFactory")
    .master("local[*]")
    .getOrCreate()

  /**
   *
   * 1. Count how many cars we have
   * 2. Count how many POWERFUL cars we have (HP > 140)
   * 3. Average HP for the entire dataset
   */

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
                  Year: Date,
                  Origin: String
                )

  // 2 - read the DF from the file
  def readDF(filename: String) = spark.read
    .option("inferSchema", "true")
    .json(s"datasets/$filename")

  val carsCaseClassDF = readDF("cars.json")

  // 3 - define an encoder (importing the implicits)
  import spark.implicits._

  // 4 - convert the DF to DS
  val carsDS = carsCaseClassDF.withColumn("Year", col("Year").cast(DateType)).as[Car]

  // 1st answer
  val carsCount = carsDS.count()
  println(carsCount)

  // 2nd answer
  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)

  // 3rd answer
  println(carsDS.map(_.Horsepower.getOrElse(0L)).reduce(_+_) / carsCount)

  // Joins
  case class Guitar(id: Long, make: String, model: String, guitarType: String)
  case class GuitarPlayer(id: Long, name: String, guitars: Seq[Long], band: Long)
  case class Band(id: Long, name: String, hometown: String, year: Long)

  val guitarsDS = readDF("guitars.json").as[Guitar]
  val guitarPlayersDS = readDF("guitarPlayers.json").as[GuitarPlayer]
  val bandsDS = readDF("bands.json").as[Band]

  val guitarPlayerBandsDS: Dataset[(GuitarPlayer, Band)] = guitarPlayersDS.joinWith(bandsDS, guitarPlayersDS.col("band") === bandsDS.col("id"), "inner")

  /**
   * Join the guitarsDS and guitarPlayersDS, in an outer join
   */
  guitarPlayersDS
    .joinWith(guitarsDS.withColumnRenamed("id", "guitarId"), expr("array_contains(guitars, guitarId)"), "outer")
    .show(false)


}
