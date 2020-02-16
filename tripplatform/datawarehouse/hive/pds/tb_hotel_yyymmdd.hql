use tripplatform;

DROP TABLE if exists tb_hotel;

CREATE external TABLE tb_hotel
(
    hotel_id int,
    hotelname string,
    province string,
    city string,
    county string,
    section string,
    address string,
    hotelphoneno string,
    hotellevel string,
    company_id int
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'

STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'

OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'

location '/sqoop/btrip_pg/20180418/tb_hotel';

