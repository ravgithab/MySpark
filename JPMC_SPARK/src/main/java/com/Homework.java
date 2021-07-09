package com;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.sources.In;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.*;

public class Homework {

    public static void main(String args[]) throws InterruptedException {

        List<Integer> input_data = new ArrayList<>();
        for (int i = 2; i <= 6; i++) {
            input_data.add(i);
        }

//        input_data.add(20);
//
//        input_data.add(21);
//        input_data.add(22);
//        input_data.add(23);
//        input_data.add(24);
//        input_data.add(25);

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> readdata = sc.textFile("C:\\Users\\Administrator\\Desktop\\wordcount.txt");
        System.out.println("Total Words in File: " + readdata.count());
        System.out.println("Spark Operations : Load form CSV");

//        JavaRDD<String> rdd2 = readdata.map(x->x.split(" ")[0]);
//        rdd2.collect().forEach(System.out::println);

//        JavaRDD <String> toyotaData = AutoAllData.filter(str -> str.contains("Toyota"));
//        System.out.println("Spark Operations : Filter");
//        System.out.println("Spark Operations: FLAT MAP");
//        System.out.println(rdd2.countByValue());

        JavaRDD <String> words = readdata.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        System.out.println("Total count of Each Word : \n" + words.countByValue());

      words.cache();

//
        while(true){
            try {
                Thread.sleep(10000);
            }catch (InterruptedException e){
                e.printStackTrace();
            }
        }
//        sc.close();
    }
}