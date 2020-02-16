package com.qi.imp

import org.apache.sqoop.client.SqoopClient
import org.apache.sqoop.model.{MFromConfig, MToConfig}

/**
  * Created by ThinkPad on 2017/11/30.
  */
object CompanyImport2 {
  val url = "http://master:12000/sqoop/"
  val client = new SqoopClient(url)

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
        |from wsc.tb_company where
        |${CONDITIONS}
      """.stripMargin
    val job = client.createJob("btrip_pgdb","btrip_hdfs")
    val fromConfig = job.getFromJobConfig()
    val toConfig = job.getToJobConfig
    showFromJobConfig(fromConfig)
    showToJobConfig(toConfig)
//    fromConfig.getStringInput("fromJobConfig.schemaName").setValue("wsc")
    fromConfig.getStringInput("fromJobConfig.sql").setValue(sql)
//    fromConfig.getStringInput("fromJobConfig.boundaryQuery").setValue("false")
    fromConfig.getStringInput("fromJobConfig.partitionColumn").setValue("company_id")
//    fromConfig.getStringInput("fromJobConfig.tableName").setValue("tb_company")
    toConfig.getEnumInput("toJobConfig.outputFormat").setValue("TEXT_FILE")
    toConfig.getEnumInput("toJobConfig.compression").setValue("NONE")
    toConfig.getStringInput("toJobConfig.outputDirectory").setValue("/sqoop/btrip_pg")
    toConfig.getBooleanInput("toJobConfig.appendMode").setValue(true)
    job.setName("btrip_company")
    deleteJob("btrip_company")
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
      case e:Exception => e.printStackTrace()
    }
  }

  import org.apache.sqoop.model.MSubmission

  def startJob(jobName: String): Unit = {
    val submission = client.startJob(jobName)
    while ( {
      submission.getStatus.isRunning
    }) {
      System.out.println("程序运行中请稍候")
      try
        Thread.sleep(3000)
      catch {
        case e: InterruptedException =>
          e.printStackTrace()
      }
    }
    if (submission.getStatus.isFailure) System.out.println("job运行失败")
    else System.out.println("job运行成功")
  }
  def main(args: Array[String]): Unit = {
//    createJob()
    startJob("btrip_company")
  }
}
