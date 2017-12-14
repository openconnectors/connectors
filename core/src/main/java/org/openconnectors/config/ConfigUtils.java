package org.openconnectors.config;

public class ConfigUtils {

    public static void require(final Config config, final String key) {
        if (config.getString(key) == null) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }

    public static void require(String... keys) {

    }
}
