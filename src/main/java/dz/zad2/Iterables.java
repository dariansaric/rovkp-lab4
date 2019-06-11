package dz.zad2;

import java.util.Iterator;
import java.util.function.Function;

public class Iterables {
    private Iterables() {}

    public static int sum(Iterable<Integer> integers) {
        int sum = 0;
        for (int i : integers) {
            sum += i;
        }
        return sum;
    }

    public static <T> int sum(Iterable<T> elements, Function<? super T, Integer> keyExtractor) {
        int sum = 0;
        for (T value : elements) {
            sum += keyExtractor.apply(value);
        }
        return sum;
    }

    public static <T> int size(Iterable<T> iterable) {
        int i = 0;
        for (Iterator<T> iter = iterable.iterator(); iter.hasNext(); iter.next()) {
            i++;
        }
        return i;
    }

    public static <T> int count(Iterable<T> elements) {
        int c = 0;
        for (T v : elements) {
            c++;
        }

        return c;
    }

}