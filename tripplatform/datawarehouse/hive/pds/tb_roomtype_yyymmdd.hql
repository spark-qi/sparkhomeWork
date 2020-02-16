use tripplatform;

DROP TABLE if exists tb_roomtype;
 CREATE external TABLE tb_roomtype
 (
     room_type_id int,
     roomtype_name string,
     company_id int,
     old_room_type_id int

 )

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'

STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'

OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'

location '/sqoop/btrip_pg/20180418/tb_roomtype';