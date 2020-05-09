package com.spark.sparksql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSQLFactory extends App {

  Logger.getLogger("org").setLevel(Level.ERROR)

  val spark = SparkSession.builder()
    .appName("SparkSQLFactory")
    .master("local[*]")
    .config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    .config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()

  spark.sql("create database ananddb")
  spark.sql("use ananddb")
  val databasesDF = spark.sql("show databases")

  /**
   * Questions
   *
   * 1. Read the movies DF and store it as a Spark table in the rtjvm database.
   * 2. Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
   * 3. Show the average salaries for the employees hired in between those dates, grouped by department.
   * 4. Show the name of the best-paying department for employees hired in between those dates.
   */

  // 1
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("datasets/movies.json")

  moviesDF.write
      .mode(SaveMode.Overwrite)
      .saveAsTable("movies1")

  spark.sql(
    """
      | describe ananddb.movies1
      |""".stripMargin).show()

  spark.sql(
    """
      | select Title, Major_Genre from movies1 where Major_Genre = 'Comedy'
      |""".stripMargin
  ).show()

  // transfer tables from a DB to Spark tables
  val driver = "org.postgresql.Driver"
  val url = "jdbc:postgresql://localhost:5432/" // replace the db name
  val user = "docker"
  val password = "docker"

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("user", user)
    .option("password", password)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if (shouldWriteToWarehouse) {
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
  )


  // 2 - Count how many employees were hired in between Jan 1 1999 and Jan 1 2000.
  val countEmployees = spark.sql(
    """
      | select count(*)
      | from employees
      | where hire_date > '1999-01-01'
      | AND
      | hire_date < '2000-01-01'
      |""".stripMargin
  ).show()

  // 3 - Show the average salaries for the employees hired in between those dates, grouped by department.
  val avgSalariesBetweenDates = spark.sql(
    """
      | select de.dept_name, avg(s.salary)
      | from
      | employees e,
      | salaries s,
      | dept_emp d,
      | departments de
      | where e.emp_no = s.emp_no
      | AND
      | e.emp_no = d.emp_no
      | AND
      | e.hire_date > '1999-01-01'
      | AND
      | e.hire_date < '2000-01-01'
      | AND
      | d.dept_no = de.dept_no
      | group by d.dept_no, de.dept_name
      |""".stripMargin
  ).show()

  // 4 - Show the name of the best-paying department for employees hired in between those dates.
  val bestPayingDeparment = spark.sql(
    """
      | select de.dept_name, avg(s.salary) as average_salary
      | from
      | employees e,
      | salaries s,
      | dept_emp d,
      | departments de
      | where e.emp_no = s.emp_no
      | AND
      | e.emp_no = d.emp_no
      | AND
      | e.hire_date > '1999-01-01'
      | AND
      | e.hire_date < '2000-01-01'
      | AND
      | d.dept_no = de.dept_no
      | group by d.dept_no, de.dept_name
      | order by average_salary desc
      | limit 1
      |""".stripMargin
  ).show()


}
