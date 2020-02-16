package com.qi.read_hive

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object PhoneNumAnalysis {

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("hive")
    .enableHiveSupport()
    .getOrCreate()

  val url="jdbc:mysql://localhost:3306/db_practice"
  val properties=new Properties()
  properties.setProperty("user","root")
  properties.setProperty("password","123456")

  import spark.sql
  import spark.implicits._

  def dumpDataToHive(): Unit = {

    val phone_result = sql(
      """
        |create external table if not exists phone(
        |phone_subId int,
        |phone_sub string,
        |address string,
        |company string,
        |area_num string,
        |post_num string
        |)
        |row format delimited
        |fields terminated by ','
        |STORED AS TEXTFILE
        |location '/spark_analysis/phonesub'
      """.stripMargin)

    val userorder_result = sql(
      """
        |create external table if not exists user_order(
        |userId string,
        |phoneNum string,
        |gender string,
        |age int,
        |orderId string
        |)
        |row format delimited
        |fields terminated by '|'
        |location '/spark_analysis/userorder'
      """.stripMargin)

    //        |row format SERDE 'org.apache.hadoop.hive.serde2.RegexSerDe'
    //        |with SERDEPROPERTIES(
    //        |"input.regex" ="(.*)\\|(.*)\\|(.*)\\|(.*)\\|(.*)"
    //        |)
  }

  def analysisPhone(): Unit ={

    sql("select address, substr(phone_sub,2,7) phone_sub,company from phone").createOrReplaceTempView("phone_view")
    sql("select distinct userId,substr(phoneNum,1,7) phoneNum,gender from user_order").createOrReplaceTempView("user_order_view")
    sql("select userId,substr(phoneNum,1,7) phoneNum,gender from user_order").createOrReplaceTempView("user_order_repeat_view")

    val result = sql(
      """
        |select a.address, userNum,round(yidong_num_rate,3) yidong_num_rate,round(liantong_user_rate,3) liantong_user_rate,
        |round(dianxin_user_rate,3) dianxin_user_rate,round(male__order_rate,3) male__order_rate,
        |round(female_order_rate,3) female_order_rate
        |from
        |(select address, count(userId) userNum,
        |sum(case when instr(company,'联通')>0 then 1 else 0 end)*1.0/count(company) liantong_user_rate,
        |sum(case when instr(company,'电信')>0 then 1 else 0 end)*1.0/count(company) dianxin_user_rate
        |from phone_view inner join user_order_view on phone_sub= phoneNum group by address) a
        |inner join
        |(
        | select address,
        |sum(case when instr(company,'移动')>0 then 1 else 0 end)*1.0/count(company) yidong_num_rate,
        |sum(case when gender='m' then 1 else 0 end)*1.0/count(gender) male__order_rate,
        |sum(case when gender='f' then 1 else 0 end)*1.0/count(gender) female_order_rate
        |from phone_view inner join user_order_repeat_view on phone_sub= phoneNum group by address
        |) b
        |on a.address=b.address order by userNum desc
        """.stripMargin)

    result.show()

    result.write.mode(SaveMode.Overwrite).jdbc(url,"phone_analysis_result",properties)
    spark.stop()



//    val result = sql(
//      """
//       select address from phone_view
//      """.stripMargin)
//
//    result.show()
  }


  def main(args: Array[String]): Unit = {
//    dumpDataToHive()
    analysisPhone()
  }

}
