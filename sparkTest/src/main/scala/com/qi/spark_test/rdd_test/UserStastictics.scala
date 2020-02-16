package com.qi.spark_test.rdd_test

import org.apache.spark.{SparkConf, SparkContext}

case class UserLogAction(name:String,actionType:String,ip:String)

object UserStastictics {

  val conf=new SparkConf().setMaster("local[*]").setAppName("useraction")
  val sc=SparkContext.getOrCreate(conf)

  def userActionTimes(inputPath:String): Unit ={

    val rdd=sc.textFile(inputPath,3)
    val parseRdd=rdd.map(x=>{
      val reg="(.*?)\t(.*?)\t(.*?)".r
      val actionLog=x match {
        case reg(name,actionType,ip)=>UserLogAction(name,actionType,ip)
        case _=>null
      }
      actionLog
    }).map(x=>(x.name,x))
//    parseRdd.take(10).foreach(println)
    parseRdd.cache()

  }

  def main(args: Array[String]): Unit = {
    userActionTimes("file:///E:\\spartest\\user-logs-large.txt")
  }

}
