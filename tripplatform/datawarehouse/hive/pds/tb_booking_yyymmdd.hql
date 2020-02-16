use tripplatform;

DROP TABLE if exists tb_booking;
CREATE external TABLE tb_booking
(
    booking_id int,
    customer_id int,
    staff_id int,
    paytype string,
    paymoney string,
    valid_flag int,
    memo string,
    disable_reason string,
    operate_time timestamp,
    disable_staff_id int,
    disable_time timestamp,
    hotel_id int,
    sourcetype int,
    acct_id int,
    full_balance string,
    book_balance string,
    checkin_balance string,
    sum_fee string,
    derate_fee string,
    final_charge string,
    price_class int,
    price_type int

)

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'

STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'

OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'

location '/sqoop/btrip_pg/20180418/tb_booking';