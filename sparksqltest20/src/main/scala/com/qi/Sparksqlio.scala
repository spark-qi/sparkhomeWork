package com.qi

import org.apache.spark.sql.SparkSession

object Sparksqlio {

  val spark=SparkSession.builder().master("local[*]").appName("io")
              .config("spark.sql.warehouse.dir","/spark/warehouse").getOrCreate()

  import spark.sql
  def csvTest(): Unit ={
//    val ds=spark.read.text("file:///E:\\user-logs-large.txt")
//    ds.printSchema()
//    ds.createOrReplaceTempView("user_log")
//    val result=
//      sql( """
//        |select split(value,'\t')[0] name,
//        |split(value,'\t')[1] action_type,
//        |split(value,'\t')[2] ip
//        |from user_log
//      """.stripMargin)
//
//    result.show()
//    result.write.csv("file:///E:\\user-logs-csv")

    val reload=spark.read.csv("file:///E:\\user-logs-csv")

    reload.printSchema()
  }


  def main(args: Array[String]): Unit = {
      csvTest()
  }
}
