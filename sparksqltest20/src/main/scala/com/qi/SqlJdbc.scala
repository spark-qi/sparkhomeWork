package com.qi

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object SqlJdbc {

  val spark=SparkSession.builder().master("local[*]").appName("mysql")
    .config("spark.sql.warehouse.dir","/spark/warehouse").getOrCreate()

  import spark.sql
  import spark.implicits._
  val url="jdbc:mysql://localhost:3306/db_practice"
  val properties=new Properties()
  properties.setProperty("user","root")
  properties.setProperty("password","123456")

  def fromsql(): Unit ={

    val ds=spark.read.jdbc(url,"student","sno",1,8,2,properties)
    ds.printSchema()
    ds.show()
  }

  def mysqlStastic(): Unit ={
    val student_ds=spark.read.jdbc(url,"student","sno",1,8,2,properties)
    student_ds.createOrReplaceTempView("mysql_student")


    val sc_ds=spark.read.jdbc(url,"sc","sno",1,28,2,properties)
    sc_ds.createOrReplaceTempView("mysql_sc")

    val result=sql(
      """
        |select s.sno, sum(sc.score) score_sum ,avg(sc.score) score_avg from mysql_student s inner join mysql_sc sc
        |on s.sno=sc.sno group by s.sno order by score_sum desc
      """.stripMargin)
    result.show()

    result.write.mode(SaveMode.Overwrite).jdbc(url,"student_score_staticstic",properties)
    spark.stop()

  }

  def main(args: Array[String]): Unit = {
//    fromsql()
    mysqlStastic()
  }


}
