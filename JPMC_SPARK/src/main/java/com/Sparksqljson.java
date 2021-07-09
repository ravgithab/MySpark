package com;

import java.util.ArrayList;

import java.util.List;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Sparksqljson {

    @SuppressWarnings("resource")
    public static void main(String[] args)
    {
        //System.setProperty("hadoop.home.dir", "c:/hadoop");
        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkSession spark = SparkSession.builder().appName("testingSql").master("local[*]")
                .config("spark.sql.warehouse.dir","file:///c:/tmp/")
                .getOrCreate();


        Dataset<Row> empDf = spark.read().json("target/My_Data/customerData.json");
        empDf.show();
        empDf.printSchema();


        //Do data frame queries
        System.out.println("SELECT Demo :");
        empDf.select(col("name"),col("salary")).show();

        System.out.println("FILTER for Age == 40 :");
        empDf.filter(col("age").equalTo(40)).show();

        System.out.println("GROUP BY gender and count :");
        empDf.groupBy(col("gender")).count().show();

        System.out.println("GROUP BY deptId and find average of salary and max of age :");
        Dataset<Row> summaryData = empDf.groupBy(col("deptid"))
                .agg(avg(empDf.col("salary")), max(empDf.col("age")));
        summaryData.show();
        ////******************************************************
        ////all above 	queries using sql api

		        empDf.createOrReplaceTempView("employee");

		        Dataset<Row> summaryData_sql = spark.sql("select deptid, avg(salary), max(age) from employee group by deptid  ");
		        summaryData_sql.show();
//		*************************

//****************************************************
Dataset<Row> genderData_sql = spark.sql("select gender, count(*) from employee group by gender  ");
        genderData_sql.show();
//  **********************************************************

    }

}
