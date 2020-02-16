package com.qi

import org.apache.sqoop.client.SqoopClient

/**
  * Created by ThinkPad on 2017/12/1.
  */
object SQClient {
  val url = "http://master:12000/sqoop/"
  val client = new SqoopClient(url)
}
