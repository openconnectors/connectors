package org.openconnectors.config;

import com.google.common.collect.Range;

public class ConfigUtils {

    public static void verifyExists(final Config config, final String key) {
        if (config.getString(key) == null) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }

    public static void verifyInRange(final Config config, final String key,
                                     Comparable lowerBound,
                                     Comparable upperBound) {
        verifyExists(config, key);
        Range<Comparable> range = Range.closed(lowerBound, upperBound);
        if (range.contains((Comparable) config.getObject(key)) == false){
            throw new IllegalArgumentException("Required property '" + key + "' not in range.");
        }
    }

    public static void verifyRegexMatch(final Config config, final String key, String regex) {
        verifyExists(config, key);
        if (config.getString(key).matches(regex) == false){
            throw new IllegalArgumentException("Required property '" + key + "' does not match regex.");
        }
    }
}
