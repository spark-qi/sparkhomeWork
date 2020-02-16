package com.qi;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountJava {
    public static SparkConf conf=new SparkConf().setAppName("java word").setMaster("local[*]");
    public static JavaSparkContext sc=new JavaSparkContext(conf);

    public static void wordCount() {
        //获取rdd
        JavaRDD<String> lines = sc.textFile("/inputpath");
        //调用rdd的api方法完成数据处理过程

        JavaRDD<String> word = lines.flatMap(new linetoword());

        JavaPairRDD<String, Integer> kvrdd = word.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });
        JavaPairRDD<String, Integer> result = kvrdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        //调用rdd的action方法触发计算读取结果
        result.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println("word:" + stringIntegerTuple2._1 + ",count:" + stringIntegerTuple2._2);
            }
        });
    }
    public static class linetoword implements FlatMapFunction<String, String> {
        String[] info;
        @Override
        public Iterator<String> call(String s) throws Exception {
            info = s.split("\\s+");

            return Arrays.asList(info).iterator();
        }

    }

    public static void main(String[] args){
        wordCount();
    }


}
