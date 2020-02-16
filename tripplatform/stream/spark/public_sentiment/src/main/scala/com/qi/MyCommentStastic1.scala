package com.qi

import java.text.SimpleDateFormat
import java.util.Locale

import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.Duration

object MyCommentStastic1 {

  val streamUtil = new GetStreamingFromKafka("commentanay", "local[*]", Duration(5000))

  //维度：时间（天），酒店，平台 累计评论数
  def commentNum() = {
    //总条数,平均总评分
    val dstream = streamUtil.getKafkaStreaming(List("comment"), "comment_cnum")
    val commentNumDstream1 = dstream.map(x => {
      val regex = "(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)".r
      val format = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH)
      val format1 = new SimpleDateFormat("yyyyMMdd")
      x match {
        case regex(cDate, cName, hotelId, platForm, title, content, score, score_s, score_l, score_a, score_f, url) => Some((format1.format(format.parse(cDate)), hotelId, platForm), score.toInt)
        case _ => None
      }
    })
      .filter(_ != None)
      .map((x: Option[((String, String, String), Int)]) => (x.get._1, (x.get._2, 1)))
      .reduceByKey((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2))
      //      state: Option[(Int, Int)]
      .updateStateByKey((values: Seq[(Int, Int)], state: Option[(Int, Int)]) => {
      val totalRecord = if (values.size > 0) values(0)._2 else 0
      val totalScore = if (values.size > 0) values(0)._1 else 0
      state match {
        case Some(old) => Some(old._1 + totalRecord, old._2 + totalScore)
        case None => Some(totalRecord, totalScore)
      }
    })
      .map(x => (x._1, (x._2._1, (x._2._2 * 1.0 / x._2._1).formatted("%.2f"))))


    //各维度的平均评分
    val commentNumDstream2 = dstream.map(x => {
      val regex = "(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)".r
      val format = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH)
      val format1 = new SimpleDateFormat("yyyyMMdd")
      x match {
        case regex(cDate, cName, hotelId, platForm, title, content, score, score_s, score_l, score_a, score_f, url) => Some((format1.format(format.parse(cDate)), hotelId, platForm), (score_s.toInt, score_l.toInt, score_a.toInt, score_f.toInt))
        case _ => None
      }
    })
      .filter(_ != None)
      .map((x: Option[((String, String, String), (Int, Int, Int, Int))]) => (x.get._1, (x.get._2._1, x.get._2._2, x.get._2._3, x.get._2._4, 1)))
      .reduceByKey((x1, x2) => (x1._1 + x2._1, x1._2 + x2._2, x1._3 + x2._3, x1._4 + x2._4, x1._5 + x2._5))
      .updateStateByKey((values: Seq[(Int, Int, Int, Int, Int)], state: Option[(Int, Int, Int, Int, Int)]) => {
        val totalScore_s = if (values.size > 0) values(0)._1 else 0
        val totalScore_l = if (values.size > 0) values(0)._2 else 0
        val totalScore_a = if (values.size > 0) values(0)._3 else 0
        val totalScore_f = if (values.size > 0) values(0)._4 else 0
        val totalCount = if (values.size > 0) values(0)._5 else 0
        state match {
          case Some(old) => Some(totalScore_s + old._1, totalScore_l + old._2, totalScore_a + old._3, totalScore_f + old._4, totalCount + old._5)
          case None => Some(totalScore_s, totalScore_l, totalScore_a, totalScore_f, totalCount)
        }

      })

      .map(x => (x._1, ((x._2._1 * 1.0 / x._2._5).formatted("%.2f"), (x._2._2 * 1.0 / x._2._5).formatted("%.2f"), (x._2._3 * 1.0 / x._2._5).formatted("%.2f"), (x._2._4 * 1.0 / x._2._5).formatted("%.2f"))))


    val commentNumDstream3 = dstream.map(x => {
      val regex = "(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)\\|(.+)".r
      val format = new SimpleDateFormat("EEE MMM dd HH:mm:ss z yyyy", Locale.ENGLISH)
      val format1 = new SimpleDateFormat("yyyyMMdd")
      x match {
        case regex(cDate, cName, hotelId, platForm, title, content, score, score_s, score_l, score_a, score_f, url) => Some((format1.format(format.parse(cDate)), hotelId, platForm), score.toInt)
        case _ => None
      }
    })
      .filter(_ != None)
      .map((x: Option[((String, String, String), Int)]) => (x.get._1, (if (x.get._2 >= 5) "好评" else if (x.get._2 >= 3) "中评" else "差评", 1)))
      .combineByKey(x => x, (x1: (String, Int), x2: (String, Int)) => (x1._1, x1._2 + x2._2), (y1: (String, Int), y2: (String, Int)) => (y1._1, y1._2 + y2._2), new HashPartitioner(2))
      //       state: Option[(String,Int,Int,String)]
      //values值是从程序运行到此刻所有数据通过聚合得到的key 对应的value
      .updateStateByKey((values: Seq[(String, Int)], state: Option[(String, Int, Int, String)]) => {
      val count = if (values.size > 0) values(0)._2 else 0
      val commentName = if (values.size > 0) values(0)._1 else "无"

      if (values.size > 0) {
        val count = values(0)._2
        val commentName = values(0)._1
        state match {
          case Some(old) => Some(commentName, old._2 + count, count, (((count - old._3) * 1.0 / old._3) * 100).formatted("%.2f") + "%")
          case None => Some(commentName, count, count, "0%")
        }
      } else {
        None
      }

    })
      .filter(x => x != None)
      .map(x => (x._1, (x._2._1, x._2._2, x._2._4)))

    //    val commentNumDstream4=commentNumDstream1.join(commentNumDstream2).join(commentNumDstream3)
    //    commentNumDstream1.print(50)
    //    commentNumDstream2.print(50)
    commentNumDstream3.print(50)
    //    commentNumDstream4.print()
    streamUtil.ssc.start()
    streamUtil.ssc.awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    commentNum()
  }

}
