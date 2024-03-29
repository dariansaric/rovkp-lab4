package lab.zad1;

import lab.PollutionReading;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class Util {
    private static final Pattern FILENAME_REGEX = Pattern.compile("pollutionData(\\d+)\\.csv");

    private static Stream<String> stream = Stream.empty();
    private static long numberOfFiles;
    private static long numberOfLines;

    private Util() {
    }

    public static Stream<PollutionReading> readLinesFromDirectory(Path dir) throws IOException {
        if (!Files.isDirectory(dir)) {
            throw new IllegalArgumentException("Directory path expected, got: " + dir);
        }

        stream = Stream.empty();
        getAllLines(getFilesStream(dir));
        return stream.map(PollutionReading::parseUnchecked).filter(Objects::nonNull);
    }

    public static Stream<Path> getFilesStream(Path dir) throws IOException {
//        System.out.println("Number of files: " + files.count());
        return Files
                .list(dir)
                .filter(f ->
                        FILENAME_REGEX.matcher(f.getFileName().toString()).matches());
    }

    private static void getAllLines(Stream<Path> files) {

        files.forEach(f -> {
            try {
                stream = Stream.concat(stream, Files.lines(f, StandardCharsets.UTF_8));
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }

    public static long getNumberOfFiles() {
        return numberOfFiles;
    }

    public static long getNumberOfLines() {
        return numberOfLines;
    }
}
