package dz.zad2;

public enum Gender {
    MALE("M"),
    FEMALE("F");

    private String symbol;

    Gender(String symbol) {
        this.symbol = symbol;
    }


    public String getSymbol() {
        return symbol;
    }

}
