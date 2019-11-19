package edu.nwmissouri.chasesmith;

import org.apache.commons.io.FileUtils;
//imports
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple4;

import java.util.Arrays;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Comparator;

/**
 * Hello world!
 */
public final class App {

    private static void process(String fileName) {

        SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("JD Word Counter");

        JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);

        JavaRDD<String> inputFile = sparkContext.textFile(fileName);

        JavaRDD<String> wordsFromFile = inputFile.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        JavaPairRDD<Integer, String> countData = wordsFromFile.mapToPair(word -> new Tuple2(word, 1)).reduceByKey((x, y) -> (int) x + (int) y);

        JavaPairRDD<Integer, String> output = countData.mapToPair(pair -> new Tuple2(pair._2, pair._1)).sortByKey(Comparator.reverseOrder());

        String outputFolder = "Output";

        Path path = FileSystems.getDefault().getPath(outputFolder);

        FileUtils.deleteQuietly(path.toFile());

        countData.saveAsTextFile("Results");

        sparkContext.stop();

    }

    public static void main(String[] args) {

        if (args.length == 1) {
            process(args[0]);
        } else if (args.length == 0) {
            System.out.println("No files provided.");
            System.exit(0);
        }
        
    }
}