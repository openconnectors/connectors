package org.openconnectors.config;

public class ConfigUtils {

    public static void verifyExists(final Config config, final String key) {
        if (config.getString(key) == null) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }

    public static void verifyExists(final Config config, final String... keys) {
        for (String key : keys) {
            verifyExists(config, key);
        }
    }
}
