package com.wso2.support;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * H2 to Oracle database migration is not straight forward. This client program reads tables in H2 database and insert
 * those to Oracle database. User must pass correct database connection url, username, password for both database as
 * command line arguments. Once it create successful connection, iterate through all necessary tables in H2 database
 * of carbon server, select all records and insert to oracle database. Make sure you have create database and tables in
 * Oracle.
 */
public class H2ToOracleMigrateClient {

    /**
     * Execute H2 to Oracle database data migration
     *
     * @param h2ConnectionURL H2 db file location
     * @param h2Username H2 username
     * @param h2Password H2 password
     * @param oracleConnectionURL Oracle db server URL
     * @param oracleUsername Oracle username
     * @param oraclePassword Oracle password
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    public void migrate(String h2ConnectionURL, String h2Username, String h2Password, String oracleConnectionURL,
                        String oracleUsername, String oraclePassword) throws SQLException,
            ClassNotFoundException {
        Connection h2Connection = getH2Connection(h2ConnectionURL, h2Username, h2Password);
        Connection oracleConnection = getOracleConnection(oracleConnectionURL, oracleUsername, oraclePassword);
    }

    /**
     * Create H2 database connection
     *
     * @param connectionURL db file location
     * @param username username
     * @param password password
     * @return H2 connection object
     * @throws SQLException
     * @throws ClassNotFoundException
     */
    private Connection getH2Connection(String connectionURL, String username, String password) throws SQLException,
            ClassNotFoundException {
        Class.forName("org.h2.Driver");
        Connection connection = DriverManager.getConnection("jdbc:h2:~/test", "wso2carbon", "wso2carbon");
        return connection;
    }

    /**
     * Create Oracle database connection
     *
     * @param connectionURL db server URL
     * @param username username
     * @param password password
     * @return Oracle connection object
     * @throws ClassNotFoundException
     * @throws SQLException
     */
    private Connection getOracleConnection(String connectionURL, String username, String password) throws
            ClassNotFoundException, SQLException {
        Class.forName("oracle.jdbc.driver.OracleDriver");
        Connection connection = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521/orcl", "system",
                "oracle");
        return connection;
    }

}
