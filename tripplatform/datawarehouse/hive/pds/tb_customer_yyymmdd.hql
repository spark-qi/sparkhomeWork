use tripplatform;

DROP TABLE if exists tb_customer;

 CREATE external TABLE tb_customer
 (
     customer_id int,
     customer_name string,
     sex string,
     certificate_type string,
     certificate_no string ,
     address string,
     phoneno string,
     note string,
     taobao_account string,
     hyunit string

 )

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'

STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'

OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'

location '/sqoop/btrip_pg/20180418/tb_customer';

