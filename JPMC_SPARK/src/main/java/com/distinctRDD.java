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

public class distinctRDD {

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

        List<Integer> data = Arrays.asList(3,6,3,4,8,6);

        JavaRDD <Integer> colldata = sc.parallelize(data);
        System.out.println("Data from RDD");

        System.out.println("Spark Distinct Example :" + colldata.distinct().collect());

    }
}
