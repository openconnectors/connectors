package org.openconnectors.util;


import com.google.common.collect.Lists;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcUtilsTest {

    @Test
    public void parseWhiteListTablesAndColumnsTestValid() {
        String validTestString = "table1(col1, col2)  , table2(col1, col2, col3)," +
                                " table3(*), table4(), table5(col1)";
        Map<String, List<String>> expectedResult = new HashMap<>();
        expectedResult.put("table1", Lists.newArrayList("col1", "col2"));
        expectedResult.put("table2", Lists.newArrayList("col1", "col2", "col3"));
        expectedResult.put("table3", null);
        expectedResult.put("table4", null);
        expectedResult.put("table5", Lists.newArrayList("col1"));
        Map<String, List<String>> result = JdbcUtils.parseWhiteListTablesAndColumns(validTestString);
        Assert.assertEquals(result, expectedResult);
    }

    @Test
    public void parseBlackListTablesAndColumnsTestValid() {
        String validTestString = "table1, table2, table3   ,   table4";
        Map<String, List<String>> expectedResult = new HashMap<>();
        expectedResult.put("table1", null);
        expectedResult.put("table2", null);
        expectedResult.put("table3", null);
        expectedResult.put("table4", null);
        Map<String, List<String>> result = JdbcUtils.parseBlackListTables(validTestString);
        Assert.assertEquals(result, expectedResult);
    }

    @Test
    public void parseIncrementingTableColumnNamesTestValid() {
        String validTestString = "table1.colA, table2.colB";
        Map<String, String> expectedResult = new HashMap<>();
        expectedResult.put("table1", "colA");
        expectedResult.put("table2", "colB");
        Map<String, String> result = JdbcUtils.parseIncrementingColumns(validTestString);
        Assert.assertEquals(result, expectedResult);
    }

    @Test
    public void parseIncrementingTableColumnNamesTestSingleTable() {
        String validTestString = "table1.colA";
        Map<String, String> expectedResult = new HashMap<>();
        expectedResult.put("table1", "colA");
        Map<String, String> result = JdbcUtils.parseIncrementingColumns(validTestString);
        Assert.assertEquals(result, expectedResult);
    }
}
