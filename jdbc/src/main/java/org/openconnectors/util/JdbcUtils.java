package org.openconnectors.util;

import org.openconnectors.JdbcConfigKeys;
import org.openconnectors.config.Config;
import org.openconnectors.exceptions.JdbcSourceConfigParsingException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcUtils {
    public static String buildFromStatement(String tableName, String identifierQuoteString) {
        String defaultQuote = "";
        if (identifierQuoteString == null) {
            return "FROM " + defaultQuote + tableName + defaultQuote;
        }
        return "FROM " + tableName;
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

    public static Map<String, String> parseIncrementingColumns(String configLine) {
        Map<String, String> tableColumnNames = new HashMap<>();
        try {
            for (String tableColumn : configLine.split("\\s*,\\s*")) {
                String[] tblCol = tableColumn.split("\\.");
                String tableName = tblCol[0];
                String columnName = tblCol[1];
                tableColumnNames.put(tableName, columnName);
            }
        } catch (Exception e) {
            throw new JdbcSourceConfigParsingException("Failed to parse incrementing columns names.", e);
        }
        return tableColumnNames;
    }

    public static String buildColumnsListString(List<String> columns) {
        String columnsString = "*";
        if (columns != null && !columns.isEmpty()) {
            columnsString = String.join(", ", columns);
        }
        return columnsString;
    }

    public static DbConnectionConfig getConnectionConfig(Config config) {
        final String dbUrl = config.getString(JdbcConfigKeys.CONNECTION_URL_CONFIG);
        final String dbUser = config.getString(JdbcConfigKeys.CONNECTION_USER_CONFIG);
        final String dbPassword = config.getString(JdbcConfigKeys.CONNECTION_PASSWORD_CONFIG);
        final int dbMaxConnectionAttempts = config.getInt(JdbcConfigKeys.CONNECTION_MAX_ATTEMPT);
        final int dbConnectionRetryDelay = config.getInt(JdbcConfigKeys.CONNECTION_RETRY_DELAY);
        return DbConnectionConfig.Builder
                .newBuilder()
                .setUrl(dbUrl)
                .setUser(dbUser)
                .setPassword(dbPassword)
                .setMaxConnectionAttempts(dbMaxConnectionAttempts)
                .setConnectionRetryDelay(dbConnectionRetryDelay)
                .build();
    }
}
