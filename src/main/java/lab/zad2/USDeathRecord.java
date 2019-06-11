package lab.zad2;

import lombok.Builder;
import lombok.Value;

import java.io.Serializable;

@Builder
@Value
public class USDeathRecord implements Serializable {

    private final int monthOfDeath, age, dayOfWeekOfDeath, mannerOfDeath;
    private final String sex, maritalStatus, autopsy;


//    public USDeathRecord(String line) throws NumberFormatException {
//        String[] splitted = line.split(",");
//        monthOfDeath = Integer.parseInt(splitted[5]);
//        sex = splitted[6];
//        age = Integer.parseInt(splitted[8]);
//        maritalStatus = splitted[15];
//        dayOfWeekOfDeath = Integer.parseInt(splitted[16]);
//        mannerOfDeath = Integer.parseInt(splitted[19]);
//        autopsy = splitted[21];
//    }

    public static USDeathRecord parse(String s) {
        String[] splitted = s.split(",");
        return builder()
                .monthOfDeath(Integer.parseInt(splitted[5]))
                .sex(splitted[6])
                .age(Integer.parseInt(splitted[8]))
                .maritalStatus(splitted[15])
                .dayOfWeekOfDeath(Integer.parseInt(splitted[16]))
                .mannerOfDeath(Integer.parseInt(splitted[19]))
                .autopsy(splitted[21])
                .build();
    }

    public static USDeathRecord parseUnchecked(String s) {
        try {
            return parse(s);
        } catch (Exception e) {
            return null;
        }
    }

    public boolean isMale() {
        return sex.equals("M");
    }

    public boolean isFemale() {
        return !isMale();
    }

    public boolean wasAutopsied() {
        return autopsy.equals("Y");
    }

    public boolean isAccident() {
        return mannerOfDeath == 1;
    }
}
