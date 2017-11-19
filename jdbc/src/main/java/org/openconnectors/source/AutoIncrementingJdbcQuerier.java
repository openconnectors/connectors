package org.openconnectors.source;

import org.openconnectors.util.ConnectionProvider;
import org.openconnectors.util.JdbcUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class AutoIncrementingJdbcQuerier extends TableQuerier {

    private long cursor;
    private List<String> columnsToQuery;
    private String incrementingColumnName;
    private String fromStatement;


    public AutoIncrementingJdbcQuerier(ConnectionProvider connectionProvider, String schemaPattern, String tableName,
                                       List<String> columnsToQuery, String incrementingColumnName) throws SQLException {
        super(connectionProvider, schemaPattern, tableName);
        this.columnsToQuery = columnsToQuery;
        this.incrementingColumnName = incrementingColumnName;
        init();
    }

    private void init() throws SQLException {
        String identifierQuoteString = getConnection().getMetaData().getIdentifierQuoteString();
        fromStatement = JdbcUtils.buildFromStatement(tableName, identifierQuoteString);
        String sql = "SELECT MAX(" + incrementingColumnName + ") " + fromStatement;
        PreparedStatement statement = getConnection().prepareStatement(sql);
        ResultSet resultSet = statement.executeQuery();
        if (resultSet.next()) {
            cursor = resultSet.getLong(1);
        } else {
            cursor = Long.MIN_VALUE;
        }
        resultSet.close();
    }

    @Override
    protected ResultSet executeQuery() throws SQLException {
        getPreparedStatement().setLong(1, cursor);
        return getPreparedStatement().executeQuery();
    }

    @Override
    protected void updateCursor(int newRecordsNumber) {
        cursor += newRecordsNumber;
    }

    @Override
    protected PreparedStatement createPreparedStatement() throws SQLException {
        String columnsString = JdbcUtils.buildColumnsListString(columnsToQuery);
        String whereStatement = " WHERE " + incrementingColumnName + " > ?";
        String sql = "SELECT " + columnsString + " " + fromStatement + " " + whereStatement;
        return getConnection().prepareStatement(sql);
    }
}
