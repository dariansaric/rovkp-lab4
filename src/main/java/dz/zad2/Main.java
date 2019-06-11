package dz.zad2;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

import static dz.zad2.SerializedComparator.serialize;


public class Main {
    private static final String INPUT_FILE = "file:/home/darian/Desktop/[ROVKP]/lab4/state/*.csv";
    private static final Path OUTPUT_FILE = Paths.get("StateNames-results.txt");

    /** Apache Spark Java RDD only accepts a serialized comparator. */
    private static final SerializedComparator<Tuple2<String, Integer>> TUPLE_COMPARING_INT = serialize((p1, p2) -> Integer.compare(p1._2, p2._2));

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("ChildrenCount");

        // Set the master if not already set through the command line
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            conf.setMaster("local[2]");
        }

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<USBabyNameRecord> records = sc.textFile(INPUT_FILE)
//                .filter(l -> !l.equals("Id,Name,Year,Gender,State,Count"))
                .map(USBabyNameRecord::parseUnchecked)
                .filter(Objects::nonNull);

        // Build the string and write it out
        String result = filterRelevantResults(records);
        System.out.println(result);
        writeToFile(result);
    }

    private static String filterRelevantResults(JavaRDD<USBabyNameRecord> records) {
        StringJoiner sb = new StringJoiner("\n");

        sb.add("1) Most unpopular male name: ");
        String mostUnpopularMaleName = records
                .filter(USBabyNameRecord::isMale)
                .groupBy(USBabyNameRecord::getName)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .min(TUPLE_COMPARING_INT)
                ._1();
        sb.add(mostUnpopularMaleName);

        sb.add("2) 10 most popular female names: ");
        String most10PopularFemaleNames = records
                .filter(USBabyNameRecord::isFemale)
                .groupBy(USBabyNameRecord::getName)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .top(10, TUPLE_COMPARING_INT)
                .stream()
                .map(Tuple2::_1)
                .collect(Collectors.joining(", "));
        sb.add(most10PopularFemaleNames);

        sb.add("3) State where most children were born in 1948: ");
        String stateWithMostChildrenBorn = records
                .groupBy(USBabyNameRecord::getState)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .max(TUPLE_COMPARING_INT)
                ._1();
        sb.add(stateWithMostChildrenBorn);

        sb.add("4) Number of newborns throughout the years: ");
        JavaPairRDD<Integer, Integer> newbornsByYearRDD = records
                .groupBy(USBabyNameRecord::getYear)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .sortByKey();
        String numberOfNewbornsPerYear = newbornsByYearRDD
                .map(pair -> String.format("\n%d: %d", pair._1, pair._2))
                .reduce(String::concat);
        sb.add(numberOfNewbornsPerYear);

        // Save these few records locally as map entries for fast search
        Map<Integer, Integer> newbornsByYearMap = newbornsByYearRDD.collectAsMap();

        sb.add("5) Percentage of name 'Luke' throughout the years: ");
        String percentageOfNamePerYear = records
                .filter(record -> "Luke".equals(record.getName()))
                .groupBy(USBabyNameRecord::getYear)
                .aggregateByKey(0, (acc, values) -> Iterables.sum(values, USBabyNameRecord::getCount) + acc, Integer::sum)
                .sortByKey()
                .map(pair -> {
                    double percent = 100.0 * pair._2 / newbornsByYearMap.get(pair._1);
                    return String.format(Locale.US, "\n%d: %.2f", pair._1, percent);
                })
                .reduce(String::concat);
        sb.add(percentageOfNamePerYear);

        sb.add("6) Total number of children born: ");
        long numChildrenBorn = newbornsByYearRDD
                .map(Tuple2::_2)
                .reduce(Integer::sum);
        sb.add(Objects.toString(numChildrenBorn));

        sb.add("7) Number of unique names: ");
        long numUniqueNames = records
                .groupBy(USBabyNameRecord::getName)
                .keys()
                .count();
        sb.add(Objects.toString(numUniqueNames));

        sb.add("8) Number of unique states: ");
        long numUniqueStates = records
                .groupBy(USBabyNameRecord::getState)
                .keys()
                .count();
        return sb.add(Objects.toString(numUniqueStates)).toString();
    }

    private static void writeToFile(String text) {
        try (PrintWriter writer = new PrintWriter(Files.newBufferedWriter(OUTPUT_FILE, StandardCharsets.UTF_8))) {
            writer.println(text);
        } catch (IOException e) {
            System.err.println("Error writing to file " + OUTPUT_FILE);
            e.printStackTrace();
        }
    }

}