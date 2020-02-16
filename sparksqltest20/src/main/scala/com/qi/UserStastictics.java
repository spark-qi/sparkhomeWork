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

public class UserStastictics {

    //对每个用户,统计login次数,logout次数,view_user次数,new_tweet次数
    //每个ip上平均行为次数=用户行为次数/该用户使用过的ip数量

    public static SparkConf conf = new SparkConf().setAppName("UserStatic").setMaster("local[*]");

    public static JavaSparkContext sc = new JavaSparkContext(conf);

    public static void loginNumber() {
        JavaRDD<String> rdd = sc.textFile("file:///C:\\Users\\Administrator\\Desktop\\user-logs-large.txt");

        //1
        JavaPairRDD<String, String> rdd1 = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] strings = s.split("\\s+");
                String s1 = strings[0] + "\t" + strings[1];
                return Arrays.asList(s1).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                String[] split = stringIntegerTuple2._1.split("\\s+");

                return new Tuple2<String, String>(split[0], split[1] + "->" + stringIntegerTuple2._2);
            }
        }).reduceByKey(new Function2<String, String, String>() {
            @Override
            public String call(String s, String s2) throws Exception {
                return s + "\t" + s2;
            }
        });

        //2
        JavaPairRDD<String, Integer> user = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] strings = s.split("\\s+");
                return Arrays.asList(strings[0]).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //3
        JavaPairRDD<String, Integer> userIps = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] split = s.split("\\s+");
                String s1 = split[0] + "\t" + split[2];
                return Arrays.asList(s1).iterator();
            }
        }).distinct().mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {

                return new Tuple2<String, Integer>(s.split("\\s+")[0], 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        //4
        JavaPairRDD<String, Integer> ips = user.join(userIps).mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Integer>>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
                return new Tuple2<String, Integer>(stringTuple2Tuple2._1, stringTuple2Tuple2._2._1 / stringTuple2Tuple2._2._2);
            }
        });

        //5结果
        rdd1.join(ips).mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Integer>>, String, String>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Tuple2<String, Integer>> s2) throws Exception {

                return new Tuple2<String, String>(s2._1, s2._2._1 + "\t" + "ip" + "->" + s2._2._2);
            }
        }).foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2._1 + "\t" + stringStringTuple2._2);
            }
        });
    }

    public static void main(String[] args) {
        loginNumber();
    }
}
