package com.qi.spark_test.rdd_test.dbtest

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object ReadMysql {

  val conf=new SparkConf().setMaster("local[*]").setAppName("readAndWriteMysql")

  val sc=SparkContext.getOrCreate(conf)

  val url="jdbc:mysql://localhost:3306/db_practice"
  val driverClass="com.mysql.jdbc.Driver"
  val username="root"
  val password="123456"
  Class.forName(driverClass)
  val connectionFactory=()=>{
    DriverManager.getConnection(url,username,password)
  }

  def getStudentRddFromMysql(): Unit ={
    val studentRdd=new  JdbcRDD(sc,connectionFactory,"select * from student inner join sc on student.sno=sc.sno where sc.sno>=? and sc.sno<=?",1,11,2,
      x=>{
        val sno=x.getInt(1)
        val sname=x.getString(2)
        val sage=x.getDate(3)
        val ssex=x.getString(4)
        (sno,(sname,sname,ssex))

      })
    studentRdd.foreach(println)

  }

  def writeStudentRddFromMysql(iterator: Iterator[(String, String,String)]): Unit ={

    val conn=connectionFactory()
    val sql="insert into student(sname,sage,ssex) values(?,?,?)"
    val ps=conn.prepareStatement(sql)


  }

  def main(args: Array[String]): Unit = {
    getStudentRddFromMysql()
  }

}
