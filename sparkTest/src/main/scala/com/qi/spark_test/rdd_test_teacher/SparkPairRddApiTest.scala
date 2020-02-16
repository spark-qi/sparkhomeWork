package com.qi.spark_test.rdd_test_teacher

import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ThinkPad on 2018/4/3.
  */
object SparkPairRddApiTest {
  val conf = new SparkConf().setAppName("kvrddapitest").setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)

  def mapValuesTest() = {
    val list = List((1,"abc"),(2,"def"),(3,"aaa"),(4,"xyz"),(1,"yui"),(2,"eee"))
    val kvrdd = sc.parallelize(list)

    val result = kvrdd.mapValues(x=>x(0))
    result.foreach(println)

    val result2 = kvrdd.map(x=>(x._1,x._2(0)))
    result2.foreach(println)
  }
  def flatMapValuesTest() = {
    val list = List(
    ("1.txt","Apache JMeter may be used to test performance both on static and dynamic resources, Web dynamic applications.")
    ,("2.txt","Ability to load and performance test many different applications/server/protocol types")
    ,("3.txt","Easy correlation through ability to extract data from most popular response formats"))
    val rdd = sc.parallelize(list)
    //统计每个文件中每个单词的数量
    val prepared = rdd.flatMapValues(x=>x.split("\\s+"))
    prepared.foreach(println)
  }
  //计算rdd中元素奇数的数量和偶数的数量
  def reduceByKeyTest() = {
    val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,11,13,15))
    val kvrdd = rdd.map(x=>(if(x%2==0) "偶数" else "奇数" , x))
    //如果要使用reducebykey
    val result = kvrdd.mapValues(x=>1).reduceByKey((x1,x2)=>x1+x2)

    result.foreach(println)
  }
  def foldByKeyTest() = {
    val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,11,13,15),2)
    val kvrdd = rdd.map(x=>(if(x%2==0) "偶数" else "奇数" , x))

    val result = kvrdd.mapValues(x=>1).foldByKey(0)((x1,x2)=>x1+x2)
    result.foreach(println)
  }
  def aggregateByKeyTest() = {
    val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,11,13,15),2)
    val kvrdd = rdd.map(x=>(if(x%2==0) "偶数" else "奇数" , x))

    val result = kvrdd.aggregateByKey(0)((x1,x2)=>x1+1,(x1,x2)=>x1+x2)
    result.foreach(println)
  }
  //x1,x2都为value值,并且x=>1是将每个分区开头的value值改为1
  def combineByKeyTest() = {
    val rdd = sc.parallelize(List(1,2,3,4,5,6,7,8,11,13,15),2)
    val kvrdd = rdd.map(x=>(if(x%2==0) "偶数" else "奇数" , x))

    val result = kvrdd.combineByKey(x=>1,(x1:Int,x2:Int)=>x1+1,(x1:Int,x2:Int)=>x1+x2)
    result.foreach(println)
  }
  //计算rdd的倒排索引
  def reverseIndex() = {
    val list = List(
      ("1.txt","Apache JMeter may be used to test performance both on static and dynamic resources, Web dynamic applications.")
      ,("2.txt","Ability to Apache Apache load and performance test many different applications/server/protocol types")
      ,("3.txt","Easy correlation through Apache Apache Apache ability to extract data from most popular response formats"))
    val rdd = sc.parallelize(list)
    //要求统计结果:Apache  1.txt(1)-->2.txt(2)-->3.txt(3)
    //           JMeter   1.txt(1)

  }
  def main(args: Array[String]): Unit = {
//    mapValuesTest()
//    flatMapValuesTest()
//    reduceByKeyTest()
//    foldByKeyTest()
//    aggregateByKeyTest()
    combineByKeyTest()
  }
}
