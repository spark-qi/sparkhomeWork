package com.qi.read_hive

import java.util.Properties

import com.qi.read_hive.PhoneNumAnalysis.spark
import org.apache.spark.sql.SparkSession

object LogAnalysis {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("hive")
    .enableHiveSupport()
    .getOrCreate()

  val url="jdbc:mysql://localhost:3306/db_practice"
  val properties=new Properties()
  properties.setProperty("user","root")
  properties.setProperty("password","123456")

  import spark.sql
  import spark.implicits._

  def logAnalysis(): Unit ={

    val result=sql(
      """
        |select count(1) pv,count(distinct remote_addr) uv,
      """.stripMargin)
  }


  def main(args: Array[String]): Unit = {

  }

}
