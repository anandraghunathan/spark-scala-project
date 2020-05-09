package com.spark.datastructures

import java.time.LocalTime
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object SparkDSAndMisc {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .config("spark.master", "local[8]")
      .appName("SparkDSAndMisc")
      .getOrCreate()

    import spark.implicits._

    val a = Array(1002, 3001, 4002, 2003, 2002, 3004, 1003, 4006)

    val b = spark
      .createDataset(a)
      .withColumn("x", col("value") % 1000)

    //println("B Show")
    //b.show()

    val c = b
      .groupBy(col("x"))
      .agg(count("x"), sum("value"))
      .drop("x")
      .toDF("count", "total")
      .orderBy(col("count").desc, col("total"))
      .limit(1)

//    println("C Show")
//    println(LocalTime.now())
//    c.show()

    val userSchema = StructType(Array(
      StructField("id", IntegerType),
      StructField("firstName", StringType),
      StructField("lastName", StringType),
      StructField("birthDate", TimestampType),
      StructField("email", StringType),
      StructField("country", StringType),
      StructField("phoneNumber", StringType)
    ))

    val df1 = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("datasets/sample.csv")

    val df2 = spark.read
      .option("inferSchema", "true")
      .schema(userSchema)
      .csv("datasets/sample.csv")

    val df3 = spark.read
      .schema(userSchema)
      .option("sep", ",")
      .csv("datasets/sample.csv")

    val df4 = spark.read
      .schema(userSchema)
      .option("header", "true")
      .csv("datasets/sample.csv")

//    df.show()
//    println(LocalTime.now())

    val rawData = Seq(
      (1, 1000, "Apple", 0.76),
      (2, 1000, "Apple", 0.11),
      (1, 2000, "Orange", 0.98),
      (1, 3000, "Banana", 0.24),
      (2, 3000, "Banana", 0.99)
    )
    val dfA = spark.createDataFrame(rawData).toDF("UserKey", "ItemKey", "ItemName", "Score")

    dfA.groupBy("UserKey")
      .agg(sort_array(collect_list(struct("Score", "ItemKey", "ItemName")), false))
      .toDF("UserKey", "Collection")
      //.show(20, false)

    val people = Seq(
      ("Ali", 0, Seq(100)),
      ("Barbara", 1, Seq(300, 250, 100)),
      ("Cesar", 1, Seq(350, 100)),
      ("Dongmei", 1, Seq(400, 100)),
      ("Eli", 2, Seq(250)),
      ("Florita", 2, Seq(500, 300, 100)),
      ("Gatimu", 3, Seq(300, 100))
    ).toDF("name", "department", "score")

    //people.show()

    val maxDept = people
      .withColumn("score", explode(col("score")))
      .groupBy("department")
      .max("score")
      .withColumnRenamed("max(score)", "highest")

    maxDept
      .join(people, "department")
      .select("name", "department", "highest")
      .dropDuplicates("department")
      .orderBy("department")
      //.show()

    import org.apache.spark.sql.expressions.Window

    val windowSpec = Window.partitionBy("department").orderBy(col("score").desc)

    people
      .withColumn("score", explode(col("score")))
      .select(
        col("department"),
        col("name"),
        dense_rank().over(windowSpec).alias("rank"),
        max(col("score")).over(windowSpec).alias("highest")
      )
      .where(col("rank") === 1)
      .drop("rank")
      .orderBy("department")
      .show()
  }
}
