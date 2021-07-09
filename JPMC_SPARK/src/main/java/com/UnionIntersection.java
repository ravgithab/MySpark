package com;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.*;

public class UnionIntersection {

    public static void main(String args[]) throws InterruptedException {

        List<Integer> input_data1 = new ArrayList<>();
        for (int i = 2; i <= 6; i++) {
            input_data1.add(i);
        }

        List<Integer> input_data2 = new ArrayList<>();
        for (int i = 4; i <= 8; i++) {
            input_data2.add(i);
        }

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        JavaRDD<Integer> rdd1 = sc.parallelize(input_data1);
        JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("hello", "war","peace","world"));
//        JavaRDD<Integer> rdd2 = sc.parallelize(input_data2).union(rdd1).distinct();
        JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("hello", "beach","peace","hill"));
//        JavaRDD<String> rdd3 = sc.parallelize(input_data2).intersection(rdd1).distinct();
        JavaRDD<String> rdd3 = rdd1.union(rdd2);
//        System.out.println("Union Numbers are:");
        System.out.println("Union Names are:");
        rdd3.collect().forEach(System.out::println);
        JavaRDD<String> rdd4 = rdd1.intersection(rdd2);
        System.out.println("Intersection Names are:");
        rdd4.collect().forEach(System.out::println);

//        System.out.println("Intersections are:");
//        rdd3.collect().forEach(System.out::println);
        sc.close();
    }
}
