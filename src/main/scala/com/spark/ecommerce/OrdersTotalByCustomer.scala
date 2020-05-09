package com.spark.ecommerce

import org.apache.spark.SparkContext
import org.apache.log4j._

object OrdersTotalByCustomer extends App {

  def getCustIdAndAmountSpent(line : String) = {
    val fields = line.split(",")
    val custId = fields(0).toInt
    val price = fields(2).toFloat

    (custId, price)
  }

  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc = new SparkContext("local[*]", "OrdersTotalByCustomer")

  val inputLines = sc.textFile("datasets/customer-orders.csv")

  val mappedInput = inputLines.map(getCustIdAndAmountSpent)

  val ordersTotalByCustomer = mappedInput.reduceByKey((x, y) => x + y)

  val results = ordersTotalByCustomer.collect()

  // Sorted by max amount spent by the customer
  results.sortWith(_._2 > _._2).foreach(println)

}
