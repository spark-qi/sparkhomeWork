use tripplatform;

DROP TABLE if exists tb_roominfo;

 CREATE external TABLE tb_roominfo
 (
     roominfo_id int,
     roomno string,
     alldayprice string,
     halfdayprice string,
     hourprice string,
     roomarea string,
     description string,
     note string,
     telephoneno string,
     bednumber int,
     gatecardid string,
     roomname string,
     layers_id int,
     room_type_id int,
     cleaning_flag int,
     valid_flag int

 )

ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'

STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'

OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'

location '/sqoop/btrip_pg/20180418/tb_roominfo';