create external table if not exists user_logs
(
    datetime string
    user_id  int,
    order_id int,
    totalMoney double,
    job string
)
row format delimited
FIELDS TERMINATED BY '|'
LINES TERMINATED BY '\n'
STORED AS textFile
location '/userstatusorder';

create table if not exists user_logs_p
{
    datetime string
    user_id  int,
    order_id int,
    totalMoney double,
    job string
}
 partitioned by (oper_date string)

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;

  insert overwrite table user_logs_p partition(oper_date)
  select  datetime
          user_id ,
          order_id ,
          totalMoney ,
          job ,
          datetime as oper_date
  from user_logs