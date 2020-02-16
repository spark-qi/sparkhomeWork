package com.qi

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext}

/**
  * Created by ThinkPad on 2018/4/19.
  */
class GetStreamingFromKafka(val appName:String, val master:String, val duration:Duration) {
  val conf = new SparkConf().setAppName(appName).setMaster(master)
  val ssc =  new StreamingContext(conf, duration)
  val sc = ssc.sparkContext
  //设置checkpoint路径
  ssc.checkpoint("file:///e:/checkpoint3")

  //从kafka中获取流数据
  def getKafkaStreaming(topics:List[String], groupId:String) ={
    val kafkaParams = Map[String,String](
      "bootstrap.servers"->"master:9092,slave1:9092,slave2:9092"
      ,"group.id"->groupId
      ,"key.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer"
      ,"value.deserializer"->"org.apache.kafka.common.serialization.StringDeserializer"

    )
    KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](topics,kafkaParams))
    .map(x=>x.value())
  }
}
