package dz.util;

import lombok.Builder;
import lombok.Value;

import java.util.Comparator;
import java.util.Objects;
import java.util.StringJoiner;

@Value
@Builder
public class SensorscopeReading implements Comparable<SensorscopeReading> {

    public static final Comparator<SensorscopeReading> EPOCH_COMPARATOR = Comparator.comparingLong(SensorscopeReading::getTimeSinceEpoch);
    private static final String PARSE_DELIMITER_REGEX = "\\s";

    private final long stationID;
    private final int year;
    private final int month;
    private final int day;
    private final int hour;
    private final int minute;
    private final int second;
    private final long timeSinceEpoch;
    private final long sequenceNumber;
    private final double configSamplingTime;
    private final double dataSamplingTime;
    private final double radioDutyCycle;
    private final double radioTransmissionPower;
    private final double radioTransmissionFrequency;
    private final double primaryBufferVoltage;
    private final double secondaryBufferVoltage;
    private final double solarPanelCurrent;
    private final double globalCurrent;
    private final double energySource;


    public static SensorscopeReading parse(String s) {
        try {
            String[] args = s.split(PARSE_DELIMITER_REGEX);
            return builder()
                    .stationID(Long.parseLong(args[0]))
                    .year(Integer.parseInt(args[1]))
                    .month(Integer.parseInt(args[2]))
                    .day(Integer.parseInt(args[3]))
                    .hour(Integer.parseInt(args[4]))
                    .minute(Integer.parseInt(args[5]))
                    .second(Integer.parseInt(args[6]))
                    .timeSinceEpoch(Long.parseLong(args[7]))
                    .sequenceNumber(Long.parseLong(args[8]))
                    .configSamplingTime(Double.parseDouble(args[9]))
                    .dataSamplingTime(Double.parseDouble(args[10]))
                    .radioDutyCycle(Double.parseDouble(args[11]))
                    .radioTransmissionPower(Double.parseDouble(args[12]))
                    .radioTransmissionFrequency(Double.parseDouble(args[13]))
                    .primaryBufferVoltage(Double.parseDouble(args[14]))
                    .secondaryBufferVoltage(Double.parseDouble(args[15]))
                    .solarPanelCurrent(Double.parseDouble(args[16]))
                    .globalCurrent(Double.parseDouble(args[17]))
                    .energySource(Double.parseDouble(args[18]))
                    .build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid argument: " + s);
        }
    }


    public static SensorscopeReading parseUnchecked(String s) {
        try {
            return parse(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }


    public String toCSV() {
        return new StringJoiner("'")
                .add(Objects.toString(stationID))
                .add(Objects.toString(year))
                .add(Objects.toString(month))
                .add(Objects.toString(day))
                .add(Objects.toString(hour))
                .add(Objects.toString(minute))
                .add(Objects.toString(second))
                .add(Objects.toString(timeSinceEpoch))
                .add(Objects.toString(sequenceNumber))
                .add(Objects.toString(configSamplingTime))
                .add(Objects.toString(dataSamplingTime))
                .add(Objects.toString(radioDutyCycle))
                .add(Objects.toString(radioTransmissionPower))
                .add(Objects.toString(radioTransmissionFrequency))
                .add(Objects.toString(primaryBufferVoltage))
                .add(Objects.toString(secondaryBufferVoltage))
                .add(Objects.toString(solarPanelCurrent))
                .add(Objects.toString(globalCurrent))
                .add(Objects.toString(energySource)).toString();
    }

    @Override
    public int compareTo(SensorscopeReading other) {
        return EPOCH_COMPARATOR.compare(this, other);
    }
}
