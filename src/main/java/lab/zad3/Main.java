package lab.zad3;

import lab.PollutionReading;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.NoSuchElementException;
import java.util.Objects;

public class Main {


    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SparkStreamingLab");

        try {
            conf.get("spark.master");
        } catch (NoSuchElementException e) {
            conf.setMaster("local[2]");
        }

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(3));

        JavaDStream<PollutionReading> records = jssc.socketTextStream("localhost", SensorStreamGenerator.PORT)
                .map(PollutionReading::parseUnchecked)
                .filter(Objects::nonNull);

        records
                .mapToPair(p -> new Tuple2<>(p.getLongitude(), p.getOzone()))
                .reduceByKeyAndWindow(Integer::min, Durations.seconds(45), Durations.seconds(15));


    }
}
