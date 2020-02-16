package com.qi.link

import java.util

import com.qi.SQClient
import org.apache.sqoop.model.{MConfig, MInput}

/**
  * Created by ThinkPad on 2017/11/30.
  */
object LinkCreator {
  val client = SQClient.client

  //创建hdfslink
  //创建postgresql link jdbc link
  def createHdfsLink() = {
    val hdfsLink = client.createLink("hdfs-connector")
    val linkConfig = hdfsLink.getConnectorLinkConfig()
    val configs: util.List[MConfig] = linkConfig.getConfigs()
    printLinkConfiguration(configs)
    linkConfig.getStringInput("linkConfig.uri").setValue("hdfs://master:9000")
    //hadoop的配置文件路径
    linkConfig.getStringInput("linkConfig.confDir").setValue("/opt/SoftWare/hadoop-2.7.3/etc/hadoop")
//    linkConfig.getMapInput("linkConfig.configOverrides").setValue()
    hdfsLink.setName("btrip_hdfs")

      deleteLink("btrip_hdfs")

    val status = client.saveLink(hdfsLink)
    if(status.canProceed){
      println("hdfs-link创建成功。")
    }else{
      println("hdfs-link创建失败。")
    }
  }
  def createPostgresqlLink() = {
    val pglink = client.createLink("generic-jdbc-connector")
    val linkConfig = pglink.getConnectorLinkConfig
    printLinkConfiguration(linkConfig.getConfigs)
    //将postgresql驱动放入sqoop lib目录中
    linkConfig.getStringInput("linkConfig.jdbcDriver").setValue("org.postgresql.Driver")
    linkConfig.getStringInput("linkConfig.connectionString").setValue("jdbc:postgresql://192.168.6.187:5432/WscHMS")
    linkConfig.getStringInput("linkConfig.username").setValue("postgres")
    linkConfig.getStringInput("linkConfig.password").setValue("123456")
    linkConfig.getStringInput("dialect.identifierEnclose").setValue(" ")
    pglink.setName("btrip_pgdb")

      deleteLink("btrip_pgdb")

    val status = client.saveLink(pglink)
    if(status.canProceed){
      println("postgresql-link创建成功。")
    }else{
      println("postgresql-link创建失败。")
    }
  }
  //打印link的配置项参数名称和参数类型等信息，方便link的配置
  def printLinkConfiguration(configs:util.List[MConfig]) = {
    for(i <- 0 until configs.size()){
      val inputs: util.List[MInput[_]] = configs.get(i).getInputs
      for(j <- 0 until inputs.size()){
        val input = inputs.get(j)
        println(input)
      }
    }
  }
  def deleteLink(name:String) = {
    try{
      client.deleteLink(name)
    }catch{
      case e:Exception => print(s"$name 不存在")
    }
  }
  def main(args: Array[String]): Unit = {
    createHdfsLink()
    createPostgresqlLink()
  }
}
