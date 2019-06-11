package lab.zad2;

public enum MaritalStatus {
    MARRIED("M"),
    DIVORCED("D"),
    WIDDOW("W");

    private String symbol;

    MaritalStatus(String s) {
        symbol = s;
    }

    public String symbol() {
        return symbol;
    }
}
