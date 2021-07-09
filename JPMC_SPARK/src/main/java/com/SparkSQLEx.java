package com;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.Partition;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import scala.Tuple2;

import javax.xml.crypto.Data;

public class SparkSQLEx {

    public static void main(String args[]) throws InterruptedException {


        Logger.getLogger("org.apache").setLevel(Level.WARN);

        //RDD Method to Ceate Spark Contetx
        //	SparkConf conf = new SparkConf().setAppName("startingSpark").setMaster("local[*]");
        //	JavaSparkContext sc = new JavaSparkContext(conf);

        //****************************//
        ///SPARK SQL We Need SPARK SESSION Obj *********** ??????////

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();

        //spark.read ---> See All Options-- read csv,json, txt , jdbc  etc...

        Dataset<Row> dataset = spark.read().option("header", true).csv("target/My_Data/students.csv");

        dataset.show();
        //dataset.show(100)

        long num_of_Rows=dataset.count();

        System.out.println("There are " + num_of_Rows + "rows");

        //To select First Row
        Row firstrow= dataset.first();
        System.out.println( "The First Row is" +firstrow+"....");
        int year = Integer.parseInt(firstrow.getAs("year").toString());
        System.out.println("Year is " + year);

// To select First Column
//        String subject = firstrow.get(2).toString();
//        System.out.println("3rd column is " + subject);

//        String subject1 = firstrow.getAs("subject").toString();
//        System.out.println("3rd column is " + subject1);

//        Column subjcolumn = dataset.col("subject");
//        Column Yearcolumn = dataset.col("year");
//        Dataset<Row> filtered = dataset.filter("subject== 'Modern Art'");
//        filtered.show();
//
//        Dataset<Row> filtered1 =dataset.filter(("subject=='Modern Art' AND year >=2007"));
//        filtered1.show(25);

        //Filters Using Lambda Expressions

//        Dataset<Row> filtered2 = dataset.filter(Row->Row.getAs("subject").equals("Modern Art") && Row.getAs("year").equals("2007"));
//        filtered2.show(10);
//
//        Dataset<Row> filtered3 = dataset.filter(col("subject").equalTo("Modern Art").and(col("year").geq("2007")));
//        filtered3.show(10);
//
//        Dataset<Row> filtered4 = dataset.filter((column("subject").equalTo("Modern Art").and(column("year").geq("2007"))));
//        filtered4.show(10);
////        System.out.println("Subject column value is " +filtered);

        dataset.createOrReplaceTempView("my_students_table");
//        Dataset<Row> results = spark.sql("select * from my_students_table where subject = 'Modern Art' AND year >=2007");

//        Dataset<Row> results = spark.sql("select distinct(year) from my_students_table order by year desc");
//        Dataset<Row> results = spark.sql("select max(score) from my_students_table where subject in(select subject from my_students_table where subject = 'Modern Art')");
        Dataset<Row> results = spark.sql("select max(score) from my_students_table group by subject");


//        results.write().option("header",true).csv("target/My_Data/mystudentout");
        results.show(5);
        //Coalesce

//        results.coalesce(1).write().option("header",true).csv("target/My_Data/modernartresults1");
//
//        results.show(5);

        spark.close();
    }

}
