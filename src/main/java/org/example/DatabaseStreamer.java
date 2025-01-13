package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class DatabaseStreamer implements AutoCloseable {

    private Connection connection;

    /**
     * Constructor to establish a database connection.
     *
     * @param url      JDBC URL of the database.
     * @param user     Database username.
     * @param password Database password.
     * @throws SQLException If a database access error occurs.
     */
    public DatabaseStreamer(String url, String user, String password) throws SQLException {
        // Establish the connection
        connection = DriverManager.getConnection(url, user, password);
        // Important for streaming large data
        connection.setAutoCommit(false);
    }

    /**
     * Streams data from the database using the provided SQL query.
     *
     * @param query SQL query to execute.
     * @return ResultSet containing the query results.
     * @throws SQLException If a database access error occurs.
     */
    public ResultSet streamData(String query) throws SQLException {
        Statement stmt = connection.createStatement(
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        // Adjust fetch size based on your database and JDBC driver
        stmt.setFetchSize(1000);
        return stmt.executeQuery(query);
    }

    /**
     * Closes the database connection.
     *
     * @throws SQLException If a database access error occurs.
     */
    @Override
    public void close() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            connection.close();
        }
    }
}
