package lab.zad2;

import dz.zad2.Iterables;
import dz.zad2.SerializedComparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.StringJoiner;

import static dz.zad2.SerializedComparator.serialize;

public class Main {
    private static final SerializedComparator<Tuple2<Integer, Integer>> TUPLE_2_SERIALIZED_COMPARATOR = serialize((p1, p2) -> Integer.compare(p1._2, p2._2));
    private static final SerializedComparator<Tuple2<Integer, Double>> TUPLE_2_SERIALIZED_COMPARATOR_2 = serialize((p1, p2) -> Double.compare(p1._2, p2._2));
    private static final String INPUT_FILE = "/home/darian/Desktop/\\[ROVKP\\]/lab4/zaLab/DeathRecords.csv";

    //    private static final Path OUTPUT_FILE = Paths.get("DeathRecords")
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("DeathRecordApp");
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException e) {
            conf.setMaster("local[2]");
        }

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<USDeathRecord> records = sc.textFile(INPUT_FILE)
                .map(USDeathRecord::parseUnchecked)
                .filter(Objects::nonNull);

        System.out.println(filterResults(records));

    }

    private static String filterResults(JavaRDD<USDeathRecord> records) {
        StringJoiner s = new StringJoiner("\n");

        s.add("Broj ženskih osoba starijih od 40: " + records
                .filter(USDeathRecord::isFemale)
                .filter(r -> r.getAge() > 40)
                .count());

        s.add("Redni broj mjeseca u godini u kojem je umrlo najviše muških osoba mlađih od 50:" +
                records.filter(USDeathRecord::isMale)
                        .filter(r -> r.getAge() < 50)
                        .groupBy(USDeathRecord::getMonthOfDeath)
                        .mapToPair((PairFunction<Tuple2<Integer, Iterable<USDeathRecord>>, Integer, Integer>) integerIterableTuple2 -> new Tuple2<>(integerIterableTuple2._1, Iterables.count(integerIterableTuple2._2)))
                        .top(1, TUPLE_2_SERIALIZED_COMPARATOR)
                        .get(0)
                        ._1
        );

        s.add("Broj ženskih osoba podvrgnuto obdukciji: " + records
                .filter(USDeathRecord::isMale)
                .filter(USDeathRecord::wasAutopsied)
                .count()
        );

        s.add("Kretanje broja umrlih žena u dobi između 50 i 65 godina po danima u tjednu: " + records
                .filter(USDeathRecord::isFemale)
                .filter(r -> r.getAge() < 65 && r.getAge() > 50)
                .groupBy(USDeathRecord::getDayOfWeekOfDeath)
                .mapToPair((PairFunction<Tuple2<Integer, Iterable<USDeathRecord>>, Integer, Integer>) integerIterableTuple2 -> new Tuple2<>(integerIterableTuple2._1, Iterables.count(integerIterableTuple2._2)))
                .top(7, TUPLE_2_SERIALIZED_COMPARATOR)
        );

        double numDeadWomenBetween50And65 = records.filter(USDeathRecord::isFemale)
                .filter(r -> r.getAge() < 65 && r.getAge() > 50).count();

        s.add("Kretanje postotka umrlih žena u dobi između 50 i 65 godina po danima u tjednu: " + records
                .filter(USDeathRecord::isFemale)
                .filter(r -> r.getAge() < 65 && r.getAge() > 50)
                .groupBy(USDeathRecord::getDayOfWeekOfDeath)
                .mapToPair((PairFunction<Tuple2<Integer, Iterable<USDeathRecord>>, Integer, Double>) integerIterableTuple2 -> new Tuple2<>(integerIterableTuple2._1, Iterables.count(integerIterableTuple2._2) / numDeadWomenBetween50And65))
                .top(7, TUPLE_2_SERIALIZED_COMPARATOR_2)
        );

        s.add("Broj umrlih muškaraca u nesreći: " + records
                .filter(USDeathRecord::isMale)
                .filter(USDeathRecord::isAccident)
                .count()
        );

        s.add("Broj različitih godina starosti umrlih osoba: " + records
                .map(USDeathRecord::getAge)
                .distinct()
                .count()
        );

        return s.toString();
    }
}
