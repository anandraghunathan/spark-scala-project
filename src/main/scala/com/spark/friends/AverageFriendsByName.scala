package com.spark.friends

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object AverageFriendsByName extends App {

  def parseLine(line : String) : (String, Int) = {

    val fields = line.split(",")

    val firstName = fields(1).toString
    val numFriends = fields(3).toInt

    (firstName, numFriends)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "AverageFriendsByName")

  val lines = sc.textFile("datasets/friends.csv")

  val rdd = lines.map(parseLine)

  val totalsByName = rdd //.filter(x => (x._1.matches("(?i)^m.*K$")))
                        .mapValues(x => (x, 1))
                        .reduceByKey((x, y) => (x._1 + y._1, x._2 + y._2))

  val averageByName = totalsByName.mapValues(x => (x._1 / x._2))

  val results = averageByName.collect()

  results.sortWith(_._2 >_._2).foreach(println)


}
