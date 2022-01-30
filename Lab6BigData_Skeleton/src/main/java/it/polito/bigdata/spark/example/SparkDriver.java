package it.polito.bigdata.spark.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class SparkDriver {

    public static void main(String[] args) {

        String inputPath;
        String outputPath;

        inputPath = args[0];
        outputPath = args[1];


//		SparkConf conf=new SparkConf().setAppName("Spark Lab #6");

        SparkConf conf = new SparkConf().setAppName("Spark Lab #6").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputRdd = sc.textFile(inputPath);

        inputRdd.filter(line -> !line.startsWith("id"));

        JavaPairRDD<String, String> userInfo = inputRdd.mapToPair(line -> {
            Tuple2<String, String> currentUserInfo;
            String[] lineValues = line.split(",");
            currentUserInfo = new Tuple2<>(lineValues[2], lineValues[1]);
            return currentUserInfo;
        }).distinct();

        JavaPairRDD<String, Iterable<String>> userProducts = userInfo.groupByKey();

        JavaRDD<Iterable<String>> transactions = userProducts.values();

        JavaPairRDD<String, Integer> pairOfProductsBoughtTogether = transactions.flatMapToPair(products -> {
            List<Tuple2<String, Integer>> flattedProducts = new ArrayList<>();
            for (String firstProduct : products) {
                for (String secondProduct : products) {
                    if (firstProduct.compareTo(secondProduct) < 0) {
                        flattedProducts.add(new Tuple2<>(firstProduct + "," + secondProduct, 1));
                    }
                }
            }
            return flattedProducts.iterator();
        });

        JavaPairRDD<String, Integer> productsFrequency = pairOfProductsBoughtTogether.reduceByKey(Integer::sum);
        JavaPairRDD<String, Integer> highFrequency = productsFrequency.filter(pair -> pair._2 > 0);
        JavaPairRDD<Integer, String> swappedHighFrequency = highFrequency.mapToPair(pair -> new Tuple2<>(pair._2(), pair._1()));
        JavaPairRDD<Integer, String> result = swappedHighFrequency.sortByKey(false);
        result.saveAsTextFile(outputPath);

        List<Tuple2<Integer, String>> top = result.take(1);
        System.out.println("Top 1");
        for (Tuple2<Integer, String> pair:  top){
            System.out.println(pair);
        }

        sc.close();
    }
}
