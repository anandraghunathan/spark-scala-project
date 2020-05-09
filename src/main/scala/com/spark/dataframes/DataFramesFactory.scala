package com.spark.dataframes

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions.{
                                        avg, col, count, countDistinct,
                                        desc_nulls_last, expr, initcap, lit, max,
                                        mean, min, regexp_extract, regexp_replace, stddev, sum
                                      }
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataFramesFactory extends App {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession.builder()
      .appName("DataframeFactory")
      .master("local[*]")
      .getOrCreate()

    /**
     * Different ways to create dataframes using rows, sequences of rows
     */
    val carsSchema = StructType(Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    ))

    val carsDF = spark.read
      //.option("inferSchema", "true")
      .schema(carsSchema)
      .json("datasets/cars.json")

    // carsDF.show()

    /**
     * A "manual" sequence of rows describing cars, fetched from cars.json in the data folder.
     */
    val cars = Seq(
      ("chevrolet chevelle malibu", 18.0, 8L, 307.0, 130L, 3504L, 12.0, "1970-01-01", "USA"),
      ("buick skylark 320", 15.0, 8L, 350.0, 165L, 3693L, 11.5, "1970-01-01", "USA"),
      ("plymouth satellite", 18.0, 8L, 318.0, 150L, 3436L, 11.0, "1970-01-01", "USA"),
      ("amc rebel sst", 16.0, 8L, 304.0, 150L, 3433L, 12.0, "1970-01-01", "USA"),
      ("ford torino", 17.0, 8L, 302.0, 140L, 3449L, 10.5, "1970-01-01", "USA"),
      ("ford galaxie 500", 15.0, 8L, 429.0, 198L, 4341L, 10.0, "1970-01-01", "USA"),
      ("chevrolet impala", 14.0, 8L, 454.0, 220L, 4354L, 9.0, "1970-01-01", "USA"),
      ("plymouth fury iii", 14.0, 8L, 440.0, 215L, 4312L, 8.5, "1970-01-01", "USA"),
      ("pontiac catalina", 14.0, 8L, 455.0, 225L, 4425L, 10.0, "1970-01-01", "USA"),
      ("amc ambassador dpl", 15.0, 8L, 390.0, 190L, 3850L, 8.5, "1970-01-01", "USA")
    )

    val carsDFManual = spark.createDataFrame(cars)
    //carsDFManual.printSchema()

    import spark.implicits._
    val carsDFManualImplicit = cars.toDF("Name", "MPG", "3", "3", "3", "3", "3", "3", "3")
    //carsDFManualImplicit.printSchema()

    val smartphone = Seq(
      ("Apple", "iPhone", "iOS", 6.1, 12),
      ("Samsung", "Galaxy S10+", "Android", 6.4, 13)
    )

    /**
     *
     *
     * 1) Create a manual DF describing smartphones
     *   - make
     *   - model
     *   - screen dimension
     *   - camera megapixels
     *
     * 2) Read another file from the data/ folder, e.g. movies.json
     *   - print its schema
     *   - count the number of rows, call count()
     */

    // 1st
    val smartphoneDF = smartphone.toDF("Make", "Model", "Platform", "Screen_Size", "Camera_Pixels")
    //smartphoneDF.show()

    // 2nd a.
    val moviesDF = spark.read.format("json")
      .option("inferSchema", "true")
      .load("datasets/movies.json")

    //moviesDF.printSchema()

    // 2nd b.
    val moviesDFCount = moviesDF.count()
    //println(s"Movies DF has a total of ${moviesDFCount} rows")

    // 1
    //    val writeMoviesDFCSV = moviesDF.write
    //      .format("csv")
    //      .option("sep", "\t")
    //      .option("header", "true")
    //      .mode(SaveMode.Overwrite)
    //      .save("datasets/moviesDF.csv")

    // 2
    //    val writeMoviesDFParquet = moviesDF.write
    //        .mode(SaveMode.Overwrite)
    //        .save("datasets/moviesDF.parquet")

    // 3
    //    val writeIntoDB = moviesDF.write
    //      .format("jdbc")
    //      .option("driver", "org.postgresql.Driver")
    //      .option("url", "jdbc:postgresql://localhost:5432/postgres")
    //      .option("user", "****")
    //      .option("password", "****")
    //      .option("dbtable", "public.movies")
    //      .mode(SaveMode.Ignore)
    //      .save()

    /**
     *
     * 1. Read the movies DF and select 2 columns of your choice
     * 2. Create another column summing up the total profit of the movies = US_Gross + Worldwide_Gross + DVD sales
     * 3. Select all COMEDY movies with IMDB rating above 6
     *
     * Use as many versions as possible
     */

    // 1
    var moviesDFWithProjections = moviesDF.selectExpr("Title", "US_Gross")
    //moviesDFWithProjections.show()

    // 2
    val moviesDFWithTotalProfit = moviesDF.select(
      col("Title"),
      col("US_Gross"),
      col("WorldWide_Gross"),
      col("US_DVD_Sales"),
      (col("US_Gross") + col("WorldWide_Gross"))
        .as("Total_Profit")
    )
    // moviesDFWithTotalProfit.show()

    val moviesDFWithTotalProfit2 = moviesDF.selectExpr("Title", "US_Gross", "WorldWide_Gross", "US_DVD_SALES",
      "US_GROSS + WorldWide_Gross as Total_Profit")
    // moviesDFWithTotalProfit2.show()

    // 3
    val bestRatedComedyMovies = moviesDF
      .select("Title", "Major_Genre", "IMDB_Rating")
      .where(
        (col("Major_Genre") === "Comedy")
          and
          (col("IMDB_Rating") > 6)
      )
    // bestRatedComedyMovies.show()

    val bestRatedComedyMovies2 = moviesDF
      .select("Title", "Major_Genre", "IMDB_Rating")
      .filter(
        "Major_Genre = 'Comedy' and IMDB_Rating > 6"
      )
    // bestRatedComedyMovies.show()

    /** Aggregations */
    val majorGenresCount = moviesDF.select(count(col("Major_Genre")))
    //majorGenresCount.show()

    val majorGenresCount2 = moviesDF.selectExpr("count(Major_Genre)")
    //majorGenresCount2.show()

    val majorGenresCountDistinct = moviesDF.selectExpr("count(DISTINCT Major_Genre) AS Distinct_Major_Genre")
    //majorGenresCountDistinct.show()

    /**
     *
     * 1. Sum up ALL the profits of ALL the movies in the DF
     * 2. Count how many distinct directors we have
     * 3. Show the mean and standard deviation of US gross revenue for the movies
     * 4. Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
     */

    // 1
    val allProfit = moviesDF.selectExpr("Title", "US_Gross + WorldWide_Gross + US_DVD_Sales AS All_Profit")
      .select(sum("All_Profit").alias("All_Profit"))
    //allProfit.show()

    // 2
    val distinctDirectors = moviesDF
      .select(countDistinct(col(("Director"))).alias("Distinct_Directors_Count"))
    //distinctDirectors.show()

    val distinctDirectors2 = moviesDF.selectExpr("count(DISTINCT Director) AS Distinct_Directors_Count")
    //distinctDirectors2.show()

    // 3
    val meanAndStddevUSGross1 = moviesDF.select(mean(col("US_Gross")).alias("Mean_US_Gross"),
      stddev(col("US_Gross")).alias("Stddev_US_Gross"))
    //meanAndStddevUSGross1.show()

    // 4
    val ratingAndRevenuePerDirector = moviesDF.select("Director", "IMDB_Rating", "US_Gross")
      .groupBy("Director")
      .agg(
        avg("IMDB_Rating").alias("Avg_Rating"),
        sum("US_Gross").alias("Total_US_Gross")
      )
      .orderBy(col("Avg_Rating").desc_nulls_last)
    //ratingAndRevenuePerDirector.show()

    /**
     *
     * 1. show all employees and their max salary
     * 2. show all employees who were never managers
     * 3. find the job titles of the best paid 10 employees in the company
     */

    val driver = "org.postgresql.Driver"
    val url = "jdbc:postgresql://localhost:5432/" // Replace with right DB name
    val user = "docker"
    val password = "docker"

    def readTable(tableName: String): sql.DataFrame = spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", s"public.$tableName")
      .load()

    val employeesDF = readTable("employees")
    val salariesDF = readTable("salaries")
    val deptManagersDF = readTable("dept_manager")
    val titlesDF = readTable("titles")

    //salariesDF.printSchema()
    // 1
    val maxSalariesDF = salariesDF.groupBy("emp_no").agg(max("salary").as("max_salary"))
    val maxSalariedEmployeesDF = employeesDF.join(maxSalariesDF, "emp_no")
    //maxSalariedEmployeesDF.show()

    // 2
    val employeesNotManagersDF = employeesDF.join(
      deptManagersDF,
      employeesDF.col("emp_no") === deptManagersDF.col("emp_no"),
      "left_anti")
    //employeesNotManagersDF.show()

    // 3
    val mostRecentTitlesPerEmployee = titlesDF.groupBy("emp_no", "title").agg(max("to_date"))
    val bestPaidEmployeesDF = maxSalariedEmployeesDF.orderBy(col("max_salary").desc).limit(10)
    val bestPaidTitlesDF = bestPaidEmployeesDF.join(mostRecentTitlesPerEmployee, "emp_no")
    //bestPaidTitlesDF.show

    /**
     *
     * Filter the cars DF by a list of car names obtained by an API call
     * Versions:
     *   - contains
     *   - regexes
     */

    def getCarNames : List[String] = List("Volkswagen", "Mercedes-Benz", "Ford")

    // 1 - Using contains (COMPLICATED logic)
    val carNamesFilter = getCarNames.map(_.toLowerCase).map(name => col("Name").contains(name))
    val bigFilter = carNamesFilter.fold(lit(false))((combinedFilter, newCarNameFilter) => combinedFilter or newCarNameFilter)
    //carsDF.filter(bigFilter).show

    // 2 - Using regex
    val regexPattern = getCarNames.map(_.toLowerCase).mkString("|")
    val isCarNameInList2 = carsDF.select(
      col("Name"),
      regexp_extract(col("Name"), regexPattern , 0).as("regex_matched")
    ).filter(col("regex_matched") =!= "").drop("regex_matched")

    //isCarNameInList2.show()

}
