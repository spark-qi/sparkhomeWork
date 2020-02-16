use tripplatform;

DROP TABLE if exists tb_buildinginfo;
 CREATE external TABLE tb_buildinginfo
 (
   buildinginfo_id int,
    siteno string,
    address string,
    hotel_id int
 )

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'

STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'

OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'

location '/sqoop/btrip_pg/20180418/tb_buildinginfo';
