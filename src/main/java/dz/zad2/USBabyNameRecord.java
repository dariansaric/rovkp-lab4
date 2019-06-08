package dz.zad2;

import lombok.Builder;
import lombok.Value;

import java.io.Serializable;
import java.util.Comparator;

@Value
@Builder
public class USBabyNameRecord implements Serializable {
    public static final Comparator<USBabyNameRecord> COUNT_COMPARATOR =
            Comparator.comparingInt(USBabyNameRecord::getCount);

    private final int id;
    private final String name;
    private final int year;
    private final Gender gender;
    private final String state;
    private final int count;

    public static USBabyNameRecord parse(String s) {
        try {
            String[] args = s.split(",");
            if (!(args[3].equals("M") || args[3].equals("F"))) {
                throw new IllegalArgumentException("Gender must be either M or F.");
            }

            return builder()
                    .id(Integer.parseInt(args[0]))
                    .name(args[1])
                    .year(Integer.parseInt(args[2]))
                    .gender(args[3].equals("M") ? Gender.MALE : Gender.FEMALE)
                    .state(args[4])
                    .count(Integer.parseInt(args[5]))
                    .build();
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid argument: " + s, e);
        }
    }

    public static USBabyNameRecord parseUnchecked(String s) {
        try {
            return parse(s);
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public boolean isMale() {
        return gender.equals(Gender.MALE);
    }

    public boolean isFemale() {
        return !isMale();
    }

}