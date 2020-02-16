package com.qi.imp

import com.qi.SQClient
import com.qi.imp.JobStarter.companyJob

/**
  * Created by ThinkPad on 2017/12/2.
  */
object JobStarter {
  //  ----导出company表
  def companyJob(): PgImport = {
    val sql =
      """
        |select company_id
        |       ,company_address
        |       ,company_attr
        |       ,company_boss
        |       ,company_name
        |       ,company_phone
        |from wsc.tb_company where
        |company_phone is not null and
        |${CONDITIONS}
      """.stripMargin
    val yyyymmdd = "20180418"
    val tableName = "tb_company"
    val pcName = "company_id"
    new PgImport(SQClient.client, yyyymmdd, sql, tableName, pcName)
  }

  def hotelJob() = {
    val sql = "select * from wsc.tb_hotel where ${CONDITIONS}"
    val yyyymmdd = "20180418"
    val tableName = "tb_hotel"
    val pcName = "hotel_id"
    new PgImport(SQClient.client, yyyymmdd, sql, tableName, pcName)
  }

  def buildingJob() = {
    val sql = "select * from wsc.tb_buildinginfo where ${CONDITIONS}"
    val yyyymmdd = "20180418"
    val tableName = "tb_buildinginfo"
    val pcName = "buildinginfo_id"
    new PgImport(SQClient.client, yyyymmdd, sql, tableName, pcName)
  }

  def layersJob() = {
    val sql = "select * from wsc.tb_layers where ${CONDITIONS}"
    val yyyymmdd = "20180418"
    val tableName = "tb_layers"
    val pcName = "layers_id"
    new PgImport(SQClient.client, yyyymmdd, sql, tableName, pcName)
  }

  def roominfoJob() = {
    val sql = "select * from wsc.tb_roominfo where ${CONDITIONS}"
    val yyyymmdd = "20180418"
    val tableName = "tb_roominfo"
    val pcName = "roominfo_id"
    new PgImport(SQClient.client, yyyymmdd, sql, tableName, pcName)
  }

  def roomTypeJob() = {
    val sql = "select * from wsc.tb_roomtype where ${CONDITIONS}"
    val yyyymmdd = "20180418"
    val tableName = "tb_roomtype"
    val pcName = "room_type_id"
    new PgImport(SQClient.client, yyyymmdd, sql, tableName, pcName)
  }

  def tbCustomerJob() = {
    val sql = "select * from wsc.tb_customer where ${CONDITIONS}"
    val yyyymmdd = "20180418"
    val tableName = "tb_customer"
    val pcName = "customer_id"
    new PgImport(SQClient.client, yyyymmdd, sql, tableName, pcName)
  }

  def tbCustSourceJob() = {
    val sql = "select * from wsc.tb_cust_source where ${CONDITIONS}"
    val yyyymmdd = "20180418"
    val tableName = "tb_cust_source"
    val pcName = "cust_source_id"
    new PgImport(SQClient.client, yyyymmdd, sql, tableName, pcName)
  }

  def tbBookingJob() = {
    val yyyymmdd = "20180418"
    val sql =
      s"""select
         |booking_id ,
         |customer_id ,
         |staff_id ,
         |paytype ,
         |paymoney ,
         |valid_flag ,
         |memo ,
         |disable_reason ,
         |to_char(operate_time,'YYYY-MM-DD HH24:MI:SS') operate_time,
         |disable_staff_id ,
         |to_char(disable_time,'YYYY-MM-DD HH24:MI:SS') disable_time,
         |hotel_id ,
         |sourcetype ,
         |acct_id ,
         |full_balance ,
         |book_balance ,
         |checkin_balance ,
         |sum_fee ,
         |derate_fee ,
         |final_charge ,
         |price_class ,
         |price_type
         |from wsc.tb_booking
         |where  to_char(operate_time, 'YYYYMMDD')='20161202' and
         |${"$"}{CONDITIONS}""".stripMargin
//    to_char(operate_time, 'YYYYMMDD')='20161202' and
    val tableName = "tb_booking"
    val pcName = "booking_id"
    new PgImport(SQClient.client, yyyymmdd, sql, tableName, pcName)
  }

  def tbBookedroomJob() = {
    val yyyymmdd = "20180418"
    val sql =
      s"""
         |select a.bookedroom_id
         |  ,a.livetype
         |  ,a.room_money
         |  ,to_char(a.booktime, 'YYYYMMDD') booktime
         |  ,a.bookvalid_flag
         |  ,a.paymoney
         |  ,a.disable_reason
         |  ,a.booking_id
         |  ,a.roominfo_id
         |  ,a.disable_staff_id
         |  ,to_char(a.disable_time,'YYYY-MM-DD HH24:MI:SS') disable_time
         |  ,a.checkin_room_id
         |from wsc.tb_bookedroom a
         |inner join wsc.tb_booking b
         |on a.booking_id = b.booking_id
         |where to_char(b.operate_time, 'YYYYMMDD')='20161202'
         |and ${"$"}{CONDITIONS}
       """.stripMargin
//    to_char(b.operate_time, 'YYYYMMDD')='20161202'
    val tableName = "tb_bookedroom"
    val pcName = "bookedroom_id"
    new PgImport(SQClient.client, yyyymmdd, sql, tableName, pcName)
  }

  def doJob(importor: PgImport) = {
    importor.createJob()
    importor.startJob()
  }

  def main(args: Array[String]): Unit = {
    val company_Job = companyJob()
    val hotel_Job = hotelJob()
    val building_Job = buildingJob()
    val layers_Job = layersJob()
    val roominfo_Job = roominfoJob()
    val roomType_Job = roomTypeJob()
    val tbCustomer_Job = tbCustomerJob()
    val tbCustSource_Job = tbCustSourceJob()
    val Booking_Job = tbBookingJob()
    val tbBookedroom_Job = tbBookedroomJob()

//    doJob(company_Job)
//    doJob(hotel_Job)
//    doJob(building_Job)
//    doJob(layers_Job)
//    doJob(roominfo_Job)
//    doJob(roomType_Job)
//    doJob(tbCustomer_Job)
//    doJob(tbCustSource_Job)
//    doJob(Booking_Job)
    doJob(tbBookedroom_Job)
  }

}
