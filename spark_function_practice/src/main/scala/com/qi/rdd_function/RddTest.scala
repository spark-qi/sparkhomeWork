package com.qi.rdd_function

import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object RddTest {

  val conf = new SparkConf().setMaster("local[*]").setAppName("rddFuctionTest")
  val sc = SparkContext.getOrCreate(conf)

  case class ScoreDetail(studentName: String, subject: String, score: Float)

  val scores = List(
    ScoreDetail("xiaoming", "Math", 98),
    ScoreDetail("xiaoming", "English", 88),
    ScoreDetail("wangwu", "Math", 75),
    ScoreDetail("wangwu", "English", 78),
    ScoreDetail("lihua", "Math", 90),
    ScoreDetail("lihua", "English", 80),
    ScoreDetail("zhangsan", "Math", 91),
    ScoreDetail("zhangsan", "English", 80))


  def main(args: Array[String]): Unit = {

    val scoresWithKey = for { i <- scores } yield { (i.studentName, i)}

    val scoresWithKeyRDD = sc.parallelize(scoresWithKey).partitionBy(new HashPartitioner(3)).cache

    scoresWithKeyRDD.foreachPartition(partition => println(partition.length))

    val avgScoreRdd= scoresWithKeyRDD.combineByKey((x: ScoreDetail) => (x.score, 1),
      (acc: (Float, Int), x: (ScoreDetail)) => (acc._1 + x.score, acc._2 + 1),
      (acc1: (Float, Int), acc2: (Float, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
    ).mapValues(x=>x._1/x._2)


    avgScoreRdd.collect().foreach(println)
   println( scoresWithKeyRDD.partitions.size)


    val rdd1=sc.parallelize(List("q,we we,r","rt,h jf","dg,df dfd,000"))
    println(rdd1.partitions.size)

    rdd1.flatMap(x=>{
      val arr=x.split("\\s+")
      arr
    }).map(x=>(x.length,x)).flatMapValues(x=>x.split(",")).foreach(println)

  }

}
