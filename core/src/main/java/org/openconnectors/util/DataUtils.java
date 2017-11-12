package org.openconnectors.util;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class DataUtils {

    public static List<String> persons = Arrays.asList(
        "Mahatma Gandhi",
        "J F Kennedy",
        "Martin Luther King",
        "Abraham Lincoln"
    );

    public static List<String> sentences = Arrays.asList(
        "I have nothing to declare but my genius",
        "You can even",
        "Compassion is an action word with no boundaries",
        "To thine own self be true"
    );

    public static String randomFromList(List<String> ls) {
        return ls.get(new Random().nextInt(ls.size()));
    }
}
