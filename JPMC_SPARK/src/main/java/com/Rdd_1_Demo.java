package com;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import org.codehaus.janino.Java;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Rdd_1_Demo {

    public static void main(String args[]) throws InterruptedException {

        List<Integer> input_data=new ArrayList<>();
        for (int i=2;i<=6;i++){
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
        JavaRDD <String> AutoAllData = sc.textFile("C:\\Users\\Administrator\\Desktop\\user.csv");
        System.out.println("Total Records: " + AutoAllData.count());
        AutoAllData.collect().forEach(System.out::println);
        System.out.println("Spark Operations : Load from CSV");
//        JavaRDD <String> tsvdata = AutoAllData.map(str -> str.replace(",","\t"));
//        tsvdata.collect().forEach(System.out::println);

        String hdr = AutoAllData.first();
//        JavaRDD <String> AutoData = AutoAllData.filter(s -> !s.equals(hdr)).filter(x->x.contains("Toyota"));
        JavaRDD <String> AutoData = AutoAllData.filter(s -> !s.equals(hdr)).map(v->new String(v.split(",")[0]));
//        System.out.println("Total Toyota Records:" + AutoData.count());
        JavaRDD <String> make_model = AutoData.distinct();
        make_model.collect().forEach(System.out::println);
       System.out.println("Total distinct cars" + make_model.count());
       System.out.println("Total distinct cars by each category" + AutoData.countByValue());

//        System.out.println("Total Distinct Records" + AutoData.distinct().collect());

//        AutoAllData.saveAsTextFile("C:\\Users\\Administrator\\Desktop\\user1.txt");
//        JavaRDD<Integer> myRDD = sc.parallelize(input_data);
//        myRDD.foreach(value-> System.out.println(value));

//        JavaRDD<Tuple2<Integer,Double>> sqRDD = myRDD.map(value->new Tuple2<>(value,Math.sqrt(value)));

//        JavaRDD <Integer> sqRDD = myRDD.map(value-> check(value));

//        myRDD.collect().forEach((System.out::println));
//        sqRDD.collect().forEach(System.out::println);
//        sqRDD.foreach(value -> System.out.println(value));
//        sqRDD.saveAsTextFile("C:\\\\Users\\\\Administrator\\\\Desktop\\\\user1.txt");
//        sqRDD.saveAsObjectFile("C:\\\\Users\\\\Administrator\\\\Desktop\\\\user3.csv");

//
//        JavaRDD<String> readFile = sc.textFile("C:\\Users\\Administrator\\Desktop\\user.txt");
//
//        System.out.println("Count is "+readFile.count());
//readFile.collect().forEach(System.out::println);
//AutoAllData.collect().forEach(System.out::println);

        sc.close();


    }
//    public static double sqroot(int st){
//        return  Math.sqrt(st);
//    }
    public static Integer check(int ck){
       int b;
        if(ck % 2 ==0){
            b = ck * ck;
        } else{
            b =ck * ck * ck;
        }
        return b;

    }
}



