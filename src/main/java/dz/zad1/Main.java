package dz.zad1;

import dz.util.SensorscopeReading;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static dz.zad1.Util.getFilesStream;
import static dz.zad1.Util.readLinesFromDirectory;

public class Main {
    private static final Path INPUT_DIR = Paths.get("./sensorscope-monitor/");
    private static final Path OUT_FILE = Paths.get("./sensorscope-monitor-all.csv");

    public static void main(String[] args) throws IOException {
        Stream<SensorscopeReading> readingStream = readLinesFromDirectory(INPUT_DIR);

        try (PrintWriter w = new PrintWriter(
                new OutputStreamWriter(
                        Files.newOutputStream(OUT_FILE), StandardCharsets.UTF_8), true)) {

            readingStream.sorted().map(SensorscopeReading::toCSV).forEach(w::println);
        } catch (IOException e) {
            System.out.println("Unable to write results, printing them on standard output instead");
            readingStream.sorted().map(SensorscopeReading::toCSV).forEach(System.out::println);
        }

        System.out.println("Number of valis files: " + getFilesStream(INPUT_DIR).count());
        System.out.println("Number of lines in output: " + Files.lines(OUT_FILE).count());
        System.out.println("Output file size in bytes: " + Files.size(OUT_FILE));
    }
}
