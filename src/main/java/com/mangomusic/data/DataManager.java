package com.mangomusic.data;

import com.mangomusic.config.DatabaseConfig;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Manages database connections for all DAOs
 * Maintains a connection pool and provides connections to DAOs
 */
public class DataManager {

    private final BasicDataSource dataSource;
    private Connection sharedConnection;

    public DataManager() {
        this.dataSource = DatabaseConfig.getDataSource();
    }

    /**
     * Gets the shared connection, creating it if needed
     * DAOs should NOT close this connection
     */
    public Connection getConnection() throws SQLException {
        if (sharedConnection == null || sharedConnection.isClosed()) {
            sharedConnection = dataSource.getConnection();
        }
        return sharedConnection;
    }

    /**
     * Closes the shared connection
     * Should be called when application exits
     */
    public void close() {
        if (sharedConnection != null) {
            try {
                sharedConnection.close();
                System.out.println("Database connection closed");
            } catch (SQLException e) {
                System.err.println("Error closing connection: " + e.getMessage());
            }
        }
    }

    public BasicDataSource getDataSource() {
        return dataSource;
    }
}