package dz.zad3;

import dz.util.SensorscopeReading;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.NoSuchElementException;
import java.util.Objects;

public class Main {
    private static final Path OUTPUT_FILE = Paths.get("./sensorscope-monitor-network");

    static {
//        System.setProperty("hadoop.home.dir", "C:\\usr\\hadoop-2.8.1");
    }

    public static void main(String[] args) {
//        Path outputFile = OUTPUT_FILE;
//        if (args.length == 1) {
//            outputFile = Paths.get(args[0]);
//        }

        SparkConf conf = new SparkConf().setAppName("SparkStreamingMaxSolarPanelCurrent");

        // Set the master if not already set through the command line
        try {
            conf.get("spark.master");
        } catch (NoSuchElementException ex) {
            // Spark streaming application requires at least 2 threads
            conf.setMaster("local[2]");
        }

        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));

        // Create a stream from text records and filter only valid records
        JavaDStream<SensorscopeReading> records = jssc.socketTextStream("localhost", SensorStreamGenerator.PORT)
                .map(SensorscopeReading::parseUnchecked)
                .filter(Objects::nonNull);

        // Do the job
        JavaPairDStream<Long, Double> result = records
//                .window(Duration.apply(60 * 1000), Duration.apply(10 * 1000))
                .mapToPair(reading -> new Tuple2<>(reading.getStationID(), reading.getSolarPanelCurrent()))
                .reduceByKeyAndWindow(Double::max, Durations.seconds(60), Durations.seconds(10));

        // Save aggregated tuples to text file
        result.dstream().saveAsTextFiles(OUTPUT_FILE.toString(), "txt");

        // Start the streaming context and wait for it to "finish"
        jssc.start();

        try {
            jssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}