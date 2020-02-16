package com.qi.imp

import com.qi.imp.CompanyImport._
import org.apache.sqoop.client.SqoopClient

/**
  * Created by ThinkPad on 2017/12/2.
  */
class PgImport(val client:SqoopClient,val yyyymmdd:String,val sql:String,val tableName:String,val pcName:String) {
  val jobName = s"btrip_${tableName}_${yyyymmdd}"

  def deleteJob() = {
    try{
      client.deleteJob(jobName)
    }catch {
      case e:Exception => print(s"$jobName 不存在")
    }
  }
  def startJob() = {
    client.startJob(jobName)
  }
  def createJob() = {
    val job = client.createJob("btrip_pgdb","btrip_hdfs")
    val fromConfig = job.getFromJobConfig()
    val toConfig = job.getToJobConfig
    showFromJobConfig(fromConfig)
    showToJobConfig(toConfig)
    fromConfig.getStringInput("fromJobConfig.sql").setValue(sql)
    fromConfig.getStringInput("fromJobConfig.partitionColumn").setValue(pcName)
    toConfig.getEnumInput("toJobConfig.outputFormat").setValue("PARQUET_FILE")//enum的直接填字符串
    toConfig.getEnumInput("toJobConfig.compression").setValue("NONE")
    toConfig.getStringInput("toJobConfig.outputDirectory").setValue(s"/sqoop/btrip_pg/$yyyymmdd/$tableName")
    toConfig.getBooleanInput("toJobConfig.appendMode").setValue(true)
    job.setName(jobName)
      deleteJob()
    val status = client.saveJob(job)
    if(status.canProceed){
      println(s"创建${jobName}成功")
    }else{
      println(status)
      println(s"创建${jobName}失败")
    }
  }
}
