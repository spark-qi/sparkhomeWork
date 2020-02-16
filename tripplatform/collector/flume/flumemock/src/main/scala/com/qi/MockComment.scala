package com.qi

import java.nio.charset.Charset
import java.util.{Date, UUID}

import scala.util.Random

object MockComment {

  val randow=new Random()
  val platForms=Array("ctrip","elong","qunar","tongcheng","tuniu","taobao","meituan")
//val platForms=Array("ctrip","elong")

  def getOneComment() :String ={

    val cDate=new Date()
    //评论者昵称
    val cName=s"customer_${randow.nextString(randow.nextInt(10)+1)}"
    //酒店id:1-100之间随机
    val hotelId=randow.nextInt(10)+1
      //随机平台
    val platForm=platForms(randow.nextInt(platForms.length))
    //标题
    val title=randow.nextString(5+randow.nextInt(10))
    //内容
    val content=randow.nextString(5+randow.nextInt(50))
    //1-5 随机总分
    val score=1+randow.nextInt(5)
    //1-5 随机服务
    val score_s=1+randow.nextInt(5)
    //1-5 随机位置
    val score_l=1+randow.nextInt(5)
    //1-5 随机设施
    val score_a=1+randow.nextInt(5)
    //1-5 随机餐饮
    val score_f=1+randow.nextInt(5)

    val url=s"http://${UUID.randomUUID()}"

    s"$cDate|$cName|$hotelId|$platForm|$title|$content|$score|$score_s|$score_l|$score_a|$score_f|$url"


  }

  def main(args: Array[String]): Unit = {
//    println(randow.nextString(6))

   for(i<-1 to 1000000000){
     val msg=getOneComment()
//    val msg= "Sat Apr 21 10:45:36 CST 2018|customer_test֒|4|qunar|oooooooooooooooo|xxxxxxxxxxxxxxxxxx|3|4|5|1|3|http://e0348815-ac13-4ffb-b51a-ea8f249d0bd7"
     FlumeClient.sendMSg(msg)
     println(msg)
     Thread.sleep(100)
   }
    FlumeClient.close()
  }
}
