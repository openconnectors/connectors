package org.openconnectors.util;

import org.openconnectors.exceptions.JdbcSourceConfigParsingException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcUtils {
    public static String buildFromStatement(String tableName, String identifierQuoteString) {
        String defaultQuote = "";
        if (identifierQuoteString == null) {
            return defaultQuote + tableName + defaultQuote;
        }
        return tableName;
    }

    public static Map<String, List<String>> parseWhiteListTablesAndColumns(String configLine) {
        try {
            Map<String, List<String>> result = new HashMap<>();
            String[] splitedConfigLine = configLine.split("\\s*\\)\\s*,\\s*");
            for (String linePart : splitedConfigLine) {
                String[] tableCols = linePart.replaceAll("\\(|\\)|\\*|,", " ")
                                             .split("\\s+");
                String tableName = tableCols[0].trim().toLowerCase();
                if (tableCols.length == 1 || tableCols[1].contains("*")) {
                    result.put(tableName, null);
                } else {
                    result.put(tableName, Arrays.asList(tableCols).subList(1, tableCols.length));
                }
            }
            return result;
        } catch (Exception e) {
            System.out.println("Throwing exception");
            throw new JdbcSourceConfigParsingException("Failed to parse tables and columns config string.", e);
        }

    }

    public static Map<String, List<String>> parseBlackListTables(String configLine) {
        Map<String, List<String>> result = new HashMap<>();
        for (String tableName : configLine.split("\\s*,\\s*")) {
            result.put(tableName.trim().toLowerCase(), null);
        }
        return result;
    }
}
