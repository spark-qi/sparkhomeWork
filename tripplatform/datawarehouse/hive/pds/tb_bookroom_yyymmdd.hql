use tripplatform;

DROP TABLE if exists tb_bookedroom;
CREATE TABLE tb_bookedroom
(
    bookedroom_id int,
    livetype string,
    room_money string,
    booktime date,
    bookvalid_flag int,
    paymoney string,
    disable_reason string,
    booking_id int,
    roominfo_id int,
    disable_staff_id int,
    disable_time timestamp,
    checkin_room_id int

)

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'

STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'

OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'

location '/sqoop/btrip_pg/20180418/tb_bookedroom';