package com;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.In;
import org.codehaus.janino.Java;
import scala.Tuple2;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class Homework1 {

    public static void main(String[] args) {


        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        JavaRDD <String> AutoAllData = sc.textFile("C:\\Users\\Administrator\\Desktop\\Data\\auto-data.csv");
        System.out.println("Total Records in Autodata: " + AutoAllData.count());
        System.out.println("Spark Operations : Load from CSV");

        String header = AutoAllData.first();
        JavaRDD <String> autodata = AutoAllData.filter(s->!s.equals(header));

        String shortest = autodata.reduce(new Function2<String, String, String>() {
            @Override
            public String call(String x, String y) throws Exception {
                return (x.length() < y.length()?x:y);
            }
        });
        System.out.println("The shortest string is :" + shortest);

//        sc.parallelize(input_data)
//                .mapToPair(value->new Tuple2<>(value.split(":") [0],1L))
//                .reduceByKey((val1, val2) -> val1 + val2)
//                .foreach(tuple -> System.out.println(tuple._1 + " " + "has" + tuple._2 + " "+ "instances"));
        sc.close();
    }
}