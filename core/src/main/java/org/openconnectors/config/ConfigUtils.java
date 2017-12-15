package org.openconnectors.config;

import java.util.Map;

public class ConfigUtils {
    public static void validate(Map<String, Object> paramMap) {
        paramMap.forEach((name, param) -> {
            if (null == param) {
                throw new IllegalArgumentException(String.format("The param %s cannot be null", name));
            }
        });
    }
}
