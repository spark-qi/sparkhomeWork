use tripplatform;

DROP TABLE if exists tb_layers;

 CREATE external TABLE tb_layers
 (
     layers_id int,
     layers_name string,
     buildinginfo_id int
 )

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'

STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'

OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'

location '/sqoop/btrip_pg/20180418/tb_layers';
