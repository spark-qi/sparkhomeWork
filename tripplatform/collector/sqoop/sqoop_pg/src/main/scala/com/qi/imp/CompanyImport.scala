package com.qi.imp

import com.qi.SQClient
import org.apache.sqoop.model.{MFromConfig, MToConfig}

/**
  * Created by ThinkPad on 2017/11/30.
  */
object CompanyImport {
  val client = SQClient.client
  var yyyymmdd = "20180418"
//  DateUtil.convert2String(new Date(),"yyyyMMdd")
  //注意sqoop的lib目录下面要把postgresql的驱动jar包拷贝进去
  //创建job job中把Company的数据导入到hdfs上
  //启动job
  def createJob() = {
    //sqoop使用的sql中必须包含${CONDITIONS}字符串
    val sql =
      """
        |select company_id
        |       ,company_address
        |       ,company_attr
        |       ,company_boss
        |       ,company_name
        |       ,company_phone
        |from wsc.tb_company
        |where company_phone is not null and
        |${CONDITIONS}
      """.stripMargin
    val job = client.createJob("btrip_pgdb","btrip_hdfs")
    val fromConfig = job.getFromJobConfig()
    val toConfig = job.getToJobConfig
    showFromJobConfig(fromConfig)
    showToJobConfig(toConfig)
//    fromConfig.getStringInput("fromJobConfig.schemaName").setValue("wsc") //用sql则这个不能写
    fromConfig.getStringInput("fromJobConfig.sql").setValue(sql)
//    fromConfig.getStringInput("fromJobConfig.boundaryQuery").setValue("false")
    fromConfig.getStringInput("fromJobConfig.partitionColumn").setValue("company_id")
//    fromConfig.getStringInput("fromJobConfig.tableName").setValue("tb_company")
    toConfig.getEnumInput("toJobConfig.outputFormat").setValue("PARQUET_FILE")//enum的直接填字符串
    toConfig.getEnumInput("toJobConfig.compression").setValue("NONE")
    //sqoop导入数据到hdfs上目录配置
    //根路径/sqoop/btrip_pg/
    // yyyymmdd/table_name
    toConfig.getStringInput("toJobConfig.outputDirectory").setValue(s"/sqoop/btrip_pg/$yyyymmdd/tb_company")
    toConfig.getBooleanInput("toJobConfig.appendMode").setValue(true)
    job.setName(s"btrip_company_$yyyymmdd")
    try {
      client.getJob(s"btrip_company_$yyyymmdd")
     deleteJob(s"btrip_company_$yyyymmdd")
    }catch {
      case e:Exception=>
    }
    val status = client.saveJob(job)
    if(status.canProceed){
      println("创建company job成功")
    }else{
      println(status)
      println("创建company job失败")
    }
  }
  //打印fromjob的配置信息项
  def showFromJobConfig(configs: MFromConfig) = {
    val configList = configs.getConfigs()
    for(i <- 0 until configList.size()){
      val config = configList.get(i)
      val inputs = config.getInputs
      for(j <- 0 until inputs.size()){
        val input = inputs.get(j)
        println(input)
      }
    }
  }
  //打印tojob的配置信息项
  def showToJobConfig(configs:MToConfig) = {
    val configList = configs.getConfigs
    for(i <- 0 until configList.size()){
      val inputs = configList.get(i).getInputs
      for(j <- 0 until inputs.size()){
        println(inputs.get(j))
      }
    }
  }
  def deleteJob(name:String) = {
    try{
      client.deleteJob(name)
    }catch {
      case e:Exception => println(s"$name job不存在")
    }
  }
  def startJob() = {
    client.startJob(s"btrip_company_$yyyymmdd")
  }
  def main(args: Array[String]): Unit = {
    yyyymmdd = args match{
      case Array(date) => date
      case _=>yyyymmdd
    }
//    createJob()
//    startJob()
    deleteJob("btrip_tb_company_20180418")
  }
}
