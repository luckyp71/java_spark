package com.explore.java_spark_examples;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/*
 * Example 1: An example on how to read data and conduct 
 * simple data manipulation from a file.
 */

public class Example1 {
	
	private static final String dataPath = "src/main/resources/dataset/animal.txt";
	
	public static void main (String[] args) {
		//Set hadoop environment
		//If you're using windows, you could download winutils from https://github.com/steveloughran/winutils
		System.setProperty("hadoop.home.dir", "D://Hadoop/hadoop-3.0.0");
		
		//Configure Spark Session
		SparkSession spark = SparkSession
							 .builder()
							 .master("local[*]")
							 .appName("Example1")
							 .getOrCreate();
		
		//Configure Dataset
		Dataset<Row> dataFrame = spark.read()
							.option("header", "true")
							.csv(dataPath);
		
		//Group data by group and age
		Dataset<Row> groupAnimalwithSameAge = dataFrame.groupBy("Group", "Age").count().select("Group", "count");
		groupAnimalwithSameAge.show();
		
		try {
			//Keep the spark web ui alive for 1 minute
			Thread.sleep(60000);		
		}catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
		  //Stop spark
		  spark.close();
		}
	}
}