package com.qi.spark_test.rdd_test_teacher

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by ThinkPad on 2018/4/4.
  */
case class UserLogAction(name:String, actionType:String, ip:String)
object UserStastictics {
  val conf = new SparkConf().setMaster("local[*]").setAppName("useraction")
  val sc = SparkContext.getOrCreate(conf)

  def userActionTimes(inputPath:String) = {
    //加载输入
    val rdd = sc.textFile(inputPath, 3)
    val parserRdd =   rdd.map(x=>{
      val reg = "(.*)\t(.*)\t(.*)".r
      val actionLog = x match{
        case reg(name,actionType,ip) => UserLogAction(name,actionType,ip)
        case _ => null
      }
      actionLog
    }).map(x=>(x.name,x))
//    parserRdd.take(10).foreach(println)
    parserRdd.cache()
    //统计login次数，logout次数，view_user次数，new_tweet次数
    val times = parserRdd.combineByKey(
      x=>(if(x.actionType=="login") 1 else 0,
        if(x.actionType=="logout") 1 else 0,
        if(x.actionType=="view_user") 1 else 0,
        if(x.actionType=="new_tweet") 1 else 0)

      ,(c:(Int,Int,Int,Int),v:UserLogAction)=>{
        (c._1+(if(v.actionType=="login") 1 else 0),
          c._2+(if(v.actionType=="logout") 1 else 0),
          c._3+(if(v.actionType=="view_user") 1 else 0),
          c._4+(if(v.actionType=="new_tweet") 1 else 0)
        )
      }

      ,(c1:(Int,Int,Int,Int),c2:(Int,Int,Int,Int))=>(c1._1+c2._1,
        c1._2+c2._2,
        c1._3+c2._3,
        c1._4+c2._4)
    )
//    times.foreach(println)
    //每个ip上平均行为次数= 用户行为次数/该用户使用过的ip数量
    //统计用户的行为次数
    val actionTimes = parserRdd.map(x=>(x._1,1)).reduceByKey(_+_)
    val userIps = parserRdd.map(x=>(x._1,x._2.ip)).distinct().map(x=>(x._1,1)).reduceByKey(_+_)
    val userIpAvgTimes = actionTimes.join(userIps).map(x=>(x._1,x._2._1*1.0/x._2._2))
    val result = times.join(userIpAvgTimes)

    result.map(x=>(x._1,x._2._1._1,x._2._1._2,x._2._1._3,x._2._1._4,x._2._2))
    .saveAsTextFile("file:///e:/useractiontimes")
  }

  def main(args: Array[String]): Unit = {
    userActionTimes("file:///E:\\spartest\\user-logs-large.txt")
  }
}
