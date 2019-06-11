package lab;

import lombok.Builder;
import lombok.Value;

import java.text.SimpleDateFormat;
import java.util.Objects;
import java.util.StringJoiner;

@Builder
@Value
public class PollutionReading implements Comparable<PollutionReading> {
    public static final SimpleDateFormat SIMPLE_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
    private static final String PARSE_DELIMITER_REGEX = ",";
    //    private static final Comparator<PollutionReading> TIMESTAMP_COMPARATOR = Date(PollutionReading::getTimestamp);
    private final int ozone;
    private final int particullateMatter;
    private final int carbonMonoxide;
    private final int sulfureDioxide;
    private final int nitrogenDioxide;
    private final double longitude;
    private final double latitude;
    private final String timestamp;

    public static PollutionReading parse(String s) {
        try {
            String[] args = s.split(PARSE_DELIMITER_REGEX);
            return builder()
                    .ozone(Integer.parseInt(args[0]))
                    .particullateMatter(Integer.parseInt(args[1]))
                    .carbonMonoxide(Integer.parseInt(args[2]))
                    .sulfureDioxide(Integer.parseInt(args[3]))
                    .nitrogenDioxide(Integer.parseInt(args[4]))
                    .longitude(Double.parseDouble(args[5]))
                    .latitude(Double.parseDouble(args[6]))
//                    .timestamp(SIMPLE_DATE_FORMAT.parse(args[7]))
                    .timestamp(args[7])
                    .build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid argument: " + s);
        }
    }

    public static PollutionReading parseUnchecked(String s) {
        try {
            return parse(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public String toCSV() {
        return new StringJoiner(",")
                .add(Objects.toString(ozone))
                .add(Objects.toString(particullateMatter))
                .add(Objects.toString(carbonMonoxide))
                .add(Objects.toString(sulfureDioxide))
                .add(Objects.toString(nitrogenDioxide))
                .add(Objects.toString(longitude))
                .add(Objects.toString(latitude))
//                .add(SIMPLE_DATE_FORMAT.format(timestamp))
                .add(timestamp)
                .toString();

    }

    @Override
    public int compareTo(PollutionReading o) {
        return timestamp.compareTo(o.timestamp);
    }
}
