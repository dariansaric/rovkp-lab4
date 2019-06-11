package lab.zad1;

import lab.PollutionReading;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static lab.zad1.Util.getFilesStream;
import static lab.zad1.Util.readLinesFromDirectory;


public class Main {
    private static final Path INPUT_DIR = Paths.get("./pollutionData/");
    private static final Path OUT_FILE = Paths.get("./pollutionData-all.csv");

    public static void main(String[] args) throws IOException {
        Stream<PollutionReading> readingStream = readLinesFromDirectory(INPUT_DIR);

        try (PrintWriter w = new PrintWriter(
                new OutputStreamWriter(
                        Files.newOutputStream(OUT_FILE), StandardCharsets.UTF_8), true)) {

            readingStream.sorted(PollutionReading::compareTo).map(PollutionReading::toCSV).forEach(w::println);
        } catch (IOException e) {
            System.out.println("Unable to write results, printing them on standard output instead");
            readingStream.sorted().map(PollutionReading::toCSV).forEach(System.out::println);
        }

        System.out.println("Number of valis files: " + getFilesStream(INPUT_DIR).count());
        System.out.println("Number of lines in output: " + Files.lines(OUT_FILE).count());
        System.out.println("Output file size in bytes: " + Files.size(OUT_FILE));
    }
}
