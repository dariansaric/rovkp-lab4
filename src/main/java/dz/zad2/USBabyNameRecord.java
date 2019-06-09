package dz.zad2;

import lombok.Builder;
import lombok.Value;

import java.io.Serializable;
import java.util.Comparator;

import static dz.zad2.Gender.FEMALE;
import static dz.zad2.Gender.MALE;

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
            if (!(args[3].equals(MALE.symbol()) || args[3].equals(FEMALE.symbol()))) {
                throw new IllegalArgumentException("Gender must be either M or F.");
            }

            return builder()
                    .id(Integer.parseInt(args[0]))
                    .name(args[1])
                    .year(Integer.parseInt(args[2]))
                    .gender(args[3].equals("M") ? MALE : Gender.FEMALE)
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
        return gender.equals(MALE);
    }

    public boolean isFemale() {
        return !isMale();
    }

}