package it.polito.bigdata.spark.example;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkDriver {

    public static void main(String[] args) {

        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);

        String inputPath;
        String outputPath;
        String prefix;

        inputPath = args[0];
        outputPath = args[1];
        prefix = args[2];


//       SparkConf conf = new SparkConf().setAppName("Spark Lab #5");
        SparkConf conf = new SparkConf().setAppName("Spark Lab #5").setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);


        JavaRDD<String> wordFreqRDD = sc.textFile(inputPath);

		/*
		 * Task 1
		 .......
		 .......
		*/
        JavaRDD<String> filteredWordFreqRDDwordFreqRDD = wordFreqRDD.filter(word -> word.startsWith(prefix));
        Long filteredNumber = filteredWordFreqRDDwordFreqRDD.count();
        JavaRDD<Integer> frequencies = filteredWordFreqRDDwordFreqRDD.map(line ->
                Integer.parseInt(line.split("\\s+")[1])).cache();
        Integer max = frequencies.reduce(Integer::max);
        System.out.println("Filtered word: " + filteredNumber);

		/*
		 * Task 2
		 .......
		 .......
		 */
        JavaRDD<String> highFrequencyWord = filteredWordFreqRDDwordFreqRDD.filter(
                (line) -> Integer.parseInt(line.split("\\s+")[1]) > max * 0.8);
        highFrequencyWord.map(line -> line.split("//s+")[0]).saveAsTextFile(outputPath);
        System.out.println("Frequent words: " + highFrequencyWord.count());
        // Close the Spark context
        sc.close();
    }
}
