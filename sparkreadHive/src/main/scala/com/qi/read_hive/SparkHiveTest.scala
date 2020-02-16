package com.qi.read_hive

import java.util.Properties

import org.apache.spark.sql.SparkSession

object SparkHiveTest {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("hive")
    .enableHiveSupport()
    .getOrCreate()

  import spark.sql
  import spark.implicits._

  val url="jdbc:mysql://localhost:3306/db_practice"
  val properties=new Properties()
  properties.setProperty("user","root")
  properties.setProperty("password","123456")

  def getHiveTable(): Unit ={
    sql("select * from bd20.dwd_ods_log").show()

    spark.stop()
  }

  def writeToHive(): Unit ={
    val ds=spark.read.jdbc("jdbc:mysql://localhost:3306/db_practice","student",properties)
    ds.createOrReplaceTempView("student")

    sql(
      """
        |create table mstudent as
        |select * from student
      """.stripMargin)
  }

  def main(args: Array[String]): Unit = {


//    getHiveTable()

      writeToHive()


  }

}
