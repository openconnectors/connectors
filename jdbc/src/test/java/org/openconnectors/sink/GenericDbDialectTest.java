package org.openconnectors.sink;

import com.google.common.collect.Lists;
import org.openconnectors.exceptions.EmptyColumnsListException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class GenericDbDialectTest {

    @Test
    public void getInsertStatementValidTest() {
        String tableName = "table1";
        String[] cls = {"col1", "col2", "col3"};
        List<String> columns = Lists.newArrayList(cls);
        GenericDbDialect dbDialect = new GenericDbDialect();
        String expectedResult = "INSERT INTO TABLE1(COL1, COL2, COL3) VALUES(?, ?, ?)";
        String result = dbDialect.getInsertStatement(tableName, columns);
        Assert.assertEquals(result, expectedResult);
    }

    @Test
    public void getInsertStatementWithOneColumnValidTest() {
        String tableName = "table1";
        List<String> columns = Lists.newArrayList("col1");
        GenericDbDialect dbDialect = new GenericDbDialect();
        String expectedResult = "INSERT INTO TABLE1(COL1) VALUES(?)";
        String result = dbDialect.getInsertStatement(tableName, columns);
        Assert.assertEquals(result, expectedResult);
    }

    @Test(expectedExceptions = EmptyColumnsListException.class)
    public void getInsertStatementWithQuotesValidTest() {
        String tableName = "table1";
        GenericDbDialect dbDialect = new GenericDbDialect();
        dbDialect.getInsertStatement(tableName, Lists.newArrayList());
    }

}
