package org.openconnectors.util;

import java.sql.Connection;
import java.sql.SQLException;

public interface ConnectionProvider {
    Connection getValidConnection() throws SQLException;
    void closeConnection();
}
