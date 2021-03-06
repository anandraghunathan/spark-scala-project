package com.spark.weather

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import scala.math.max

object LocationsWithMaxTemperature {
  def parseLine(line:String)= {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationID, entryType, temperature)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  // Create a SparkContext using every core of the local machine
  val sc = new SparkContext("local[*]", "LocationsWithMaxTemperature")

  // Read each line of input data
  val lines = sc.textFile("datasets/weather-data.csv")

  // Convert to (stationID, entryType, temperature) tuples
  val parsedLines = lines.map(parseLine)

  // Filter out all but TMIN entries
  val minTemps = parsedLines.filter(x => x._2 == "TMAX")

  // Convert to (stationID, temperature)
  val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))

  // Reduce by stationID retaining the maximum temperature found
  val maxTempsByStation = stationTemps.reduceByKey( (x,y) => max(x,y))

  // Collect, format, and print the results
  val results = maxTempsByStation.collect()

  for (result <- results.sortWith(_._2 > _._2)) {
    val station = result._1
    val temp = result._2
    val formattedTemp = f"$temp%.2f F"
    println(s"$station maximum temperature: $formattedTemp")
  }
}
