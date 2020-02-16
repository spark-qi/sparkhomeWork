package com.qi

import org.apache.spark.sql.SparkSession

case class Student(sno:Int,name:String,age:Int,address:String)

object DfDs {

  val spark=SparkSession.builder().master("local[*]").appName("datafram dataset")
              .config("spark.sql.warehouse.dir","/spark/warehouse").getOrCreate()

  import spark.sql
  import spark.implicits._

  def createDataFrame(): Unit ={

    val list=List(("张三",23,"河南"),("李四",26,"河北"),("王二",21,"北京"),("刘三",44,"上海"))
    val rdd =spark.sparkContext.parallelize(list)

//    val df=rdd.toDF()
    val df=rdd.toDF("name","age","address")
    df.createOrReplaceTempView("person")
    val result=sql(
      """
        |select age from person
      """.stripMargin)
    result.show()
    df.printSchema()
  }

  def createDataset(): Unit ={
    val list=List(("张三",23,"河南"),("李四",26,"河北"),("王二",21,"北京"),("刘三",44,"上海"))
    val rdd =spark.sparkContext.parallelize(list)
    val ds=rdd.toDS()
    ds.printSchema()
  }

  def createDataset1(): Unit ={
    val list=List(Student(1,"jim",22,"usa"),Student(2,"tom",25,"landon"))
    val rdd =spark.sparkContext.parallelize(list)
    val ds=rdd.toDS()
    ds.printSchema()
    //dataset与rdd可以相互转换
//    ds.rdd
  }

  def main(args: Array[String]): Unit = {
//    createDataFrame()
//    createDataset()
    createDataset1()
  }


}
