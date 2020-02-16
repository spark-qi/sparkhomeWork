package com.qi

import org.apache.spark.streaming.{Duration, Minutes, Seconds}

object SparkExam3 {
  val streamUtil = new GetStreamingFromKafka("commentanay","local[*]",Duration(5000))

  def commentNum() = {
    val dstream = streamUtil.getKafkaStreaming(List("test3"), "test")

    val commentNumDstream = dstream.map(x=>{
      val regex = "(.+)\\|(.+)\\|(.+)".r
      x match{
        case regex(date,catogry,rate) =>Some(date,catogry,rate.toInt)
        case _ =>None
      }
    })
      .filter(_ != None)
        .filter(x=>x.get._3>70)
      .map(x=>(x.get._2,1))
      .reduceByKeyAndWindow((v1:Int,v2:Int)=>v1+v2,Minutes(1),Seconds(10))

      .updateStateByKey((values:Seq[Int],state:Option[Int])=>{
        val batSum = if(values.size>0) values.sum else 0
        state match {
          case Some(old) => Some(old + batSum)
          case None => Some(batSum)
        }
      })

    commentNumDstream.print(20)
    streamUtil.ssc.start()
    streamUtil.ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    commentNum()
  }
}
