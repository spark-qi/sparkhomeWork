package com.qi

import java.nio.charset.Charset

import org.apache.flume.api.RpcClientFactory
import org.apache.flume.event.EventBuilder

object FlumeClient {

  val client=RpcClientFactory.getDefaultInstance("slave1",8889)

  def sendMSg(msg:String): Unit ={
    val event=EventBuilder.withBody(msg, Charset.forName("UTF-8"))
    client.append(event)
  }

 def close(): Unit ={
   if (client!=null){
     client.close()
   }
 }

}
