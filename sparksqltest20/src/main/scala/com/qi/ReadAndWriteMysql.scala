package com.qi

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object ReadAndWriteMysql {

  //计算每个学生的总成绩,平均成绩

  val conf = new SparkConf().setAppName("readMysql").setMaster("local[*]")
  val sc = SparkContext.getOrCreate(conf)

  val url = "jdbc:mysql://localhost:3306/db_practice"
  val driverClass = "com.mysql.jdbc.Driver"
  val username = "root"
  val password = "123456"
  val connectionFactory = () => {
    Class.forName(driverClass)
    DriverManager.getConnection(url, username, password)
  }

  def getStudentRddFromMysql() = {
    val studentRdd = new JdbcRDD(sc, connectionFactory, "select * from student where sno>=? and sno<=?", 1, 100, 2, x => {
      val sno = x.getInt("sno")
      val sname = x.getString("sname")
      val sage = x.getString("sage")
      val ssex = x.getString("ssex")
      (sno, (sname, sage, ssex))
    })
    studentRdd.foreach(println)
  }

  def saveRddToMysql() = {
    val rdd = sc.parallelize(List(("张三", "20180306", "a"), ("李四", "20180908", "v"), ("王五", "20180501", "d")))
    //    val newrdd = rdd.map(x => {
    //      val connection = connectionFactory()
    //      val stmt = connection.createStatement()
    //      val sql = s"insert into student(sname,sage,ssex) values('${x._1}','${x._2}','${x._3}')"
    //      stmt.executeUpdate(sql)
    //      x
    //    })
    //    newrdd.count()

    //使用mappartition把数据写入mysql
    val newrdd = rdd.mapPartitions(x => {
      val connection = connectionFactory()

      val sql = s"insert into student(sname,sage,ssex) values(?,?,?)"
      val stmt = connection.prepareStatement(sql)
      for (i <- x) {
        stmt.setString(1, i._1)
        stmt.setString(2, i._2)
        stmt.setString(3, i._3)
        stmt.addBatch()
      }
      stmt.executeBatch()
      x
    })
    //还要调用action方法
    newrdd.count()

//        val newrdd = rdd.foreachPartition(x=>{
//          val connection = connectionFactory()
//          val sql = "insert into student(sname,sage,ssex) values(?,?,?)"
//          val stmt = connection.prepareStatement(sql)
//          for(i<- x){
//            stmt.setString(1,i._1)
//            stmt.setString(2,i._2)
//            stmt.setString(3,i._3)
//            stmt.addBatch()
//          }
//          stmt.executeBatch()
//        })



  }


  //使用mappatition把数据写入mysql

  def main(args: Array[String]): Unit = {
//        getStudentRddFromMysql()
    saveRddToMysql()
  }


}
