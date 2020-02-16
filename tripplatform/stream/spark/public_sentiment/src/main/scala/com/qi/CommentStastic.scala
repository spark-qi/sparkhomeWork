package com.qi

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.streaming.Duration

/**
  * Created by ThinkPad on 2018/4/19.
  */
object CommentStastic {
  val streamUtil = new GetStreamingFromKafka("commentanay","local[*]",Duration(5000))

  //维度：时间（天），酒店，平台 累计评论数
  def commentNum() = {
    val dstream = streamUtil.getKafkaStreaming(List("comment"), "comment_cnum")
    dstream
    val commentNumDstream = dstream.map(x=>{
      val regex = "(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)".r
      val format = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy",Locale.ENGLISH)
      val format1 = new SimpleDateFormat("yyyyMMdd")
      x match{
        case regex(cDate,cName,hotelId,platForm,title,content,score,score_s,score_l,score_a,score_f,url) =>Some(format1.format(format.parse(cDate)),hotelId,platForm)
        case _ =>None
      }
    })
      .filter(_ != None)
      .map(x=>(x,1))
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
