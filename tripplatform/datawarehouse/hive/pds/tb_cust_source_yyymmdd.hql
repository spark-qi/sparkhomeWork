use tripplatform;

DROP TABLE if exists tb_cust_source;
CREATE external TABLE tb_cust_source
(
    cust_source_id int,
    hotel_id int,
    source_name string,
    status int,
    note string

)

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'

STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'

OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'

location '/sqoop/btrip_pg/20180418/tb_cust_source';


select * from tb_cust_source;
