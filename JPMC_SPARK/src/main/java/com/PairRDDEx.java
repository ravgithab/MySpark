package com;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.sources.In;
import org.codehaus.janino.Java;
import scala.Tuple2;
import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PairRDDEx {

    public static void main(String[] args) {

        List<String> input_data = new ArrayList<>();
        input_data.add("WARN: Tuesday 4 September 0504");
        input_data.add("ERROR: Tuesday 4 September 0604");
        input_data.add("FATAL: Wednessday 4 September 0704");
        input_data.add("ERROR: Friday 4 September 0804");
        input_data.add("WARN: Saturday September 0904");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD <String> rdd1 = sc.parallelize(input_data);
        JavaPairRDD<String, Long> rdd2 = rdd1.mapToPair(value -> new Tuple2<>(value.split(":") [0], 1L));

        rdd2.collect().forEach(value1 -> System.out.println(value1));
        rdd2.groupByKey().collect().forEach(value -> System.out.println(value));

        JavaPairRDD <String, Iterable<Long>> rdd3 = rdd2.groupByKey();
        rdd3.foreach(aaaa -> System.out.println(aaaa._1 + "has" + Iterables.size(aaaa._2)+"instances"));
        sc.close();
    }
}