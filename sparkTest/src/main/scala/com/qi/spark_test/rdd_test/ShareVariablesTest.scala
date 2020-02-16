package com.qi.spark_test.rdd_test

import org.apache.spark.{SparkConf, SparkContext}

object ShareVariablesTest {

  val conf = new SparkConf().setAppName("sharevariable").setMaster("local[*]")

  val sc = SparkContext.getOrCreate(conf)

  def broadcastTest(condition: Int): Unit = {

    val x = condition
    val rdd = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9), 2)

    val bCondition = sc.broadcast(x)
    //   val result= rdd.filter(i=>i>x).reduce((i1,i2)=>i1+i2)
    val result = rdd.filter(i => i > bCondition.value ).reduce((i1, i2) => i1 + i2)

    println(s"大于${x}的元素的累加值为:${result}")

  }

  def mapSideJoin(): Unit ={
    val student=sc.parallelize(List((1,"小张"),(2,"小王"),(3,"小李")))
    val score=sc.parallelize(List((1,"语文88"),(1,"数学22"),(1,"英语33"),(2,"语文88"),(2,"数学33"),(3,"语文34"),(3,"数学88"),(3,"英语88")))

    val studentCollector=student.collect().toMap

    val studentBroadCast =sc.broadcast(studentCollector)

    val result=score.map(x=>{
      val ltable=studentBroadCast.value
      val joindata=ltable(x._1)
      (x._1,joindata,x._2)
    })

    result.foreach(println)
  }

  def accumulatorTest(): Unit ={
    val rdd=sc.parallelize(List(1,2,3,4,5,6,7,8,9),2)
    val sum=rdd.reduce((x1,x2)=>x1+x2)

    val jnum=rdd.filter(x=>x%2!=0).count()

    val onum=rdd.filter(x=>x%2==0).count()

    val count=rdd.count()
    //使用累加器
    val jnuma=sc.longAccumulator("jnum")

    val onuma=sc.longAccumulator("onum")

    val counta=sc.longAccumulator("conta")

    val sumResult=rdd.map(x=>{

      counta.add(1)
      if (x%2!=0) jnuma.add(1)
      if (x%2==0) onuma.add(1)

      x
    }).reduce(_+_)

    System.out.println(s"累加值是:${sumResult},奇数的数量是:${jnuma.value}")
    System.out.println(s"总个数是:${counta.value},偶数的数量是:${onuma.value}")
  }

  def main(args: Array[String]): Unit = {
//    broadcastTest(5)
//    mapSideJoin()
    accumulatorTest()
  }
}
