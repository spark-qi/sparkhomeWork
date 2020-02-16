package com.qi

import org.apache.spark.sql.SparkSession

object SqlWordCount {

  //构建sparkSession对象
  val sparkSession=SparkSession.builder().master("local[*]").appName("sql word count")
                      .config("spark.sql.warehouse.dir","/spark/warehouse").getOrCreate()

  //sql实现wordcount功能

  def wordCount(): Unit ={
    val ds=sparkSession.read.text("file:///E:\\number.log")
    //把ds创建成视图
    ds.createOrReplaceTempView("lines")
    //打印ds的模式
    ds.printSchema()
    //对视图使用sql
   val result= sparkSession.sql(
      """
        |select word,count(1) wc
        |from(select explode(split(value,"\\s+")) word
        | from lines) a
        | group by word
      """.stripMargin)
    result.show()
    sparkSession.stop()
  }

  def main(args: Array[String]): Unit = {
    wordCount()
  }

}
