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
        Connection h2Connection = null;
        Connection oracleConnection = null;
        try {
            h2Connection = getH2Connection(h2ConnectionURL, h2Username, h2Password);
            oracleConnection = getOracleConnection(oracleConnectionURL, oracleUsername, oraclePassword);
            migrateUserManagerDatabase(h2Connection, oracleConnection);
        } finally {
            if (h2Connection != null) {
                h2Connection.close();
            }
            if (oracleConnection != null) {
                oracleConnection.close();
            }
        }
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
        return DriverManager.getConnection(connectionURL, username, password);
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
        return DriverManager.getConnection(connectionURL, username, password);
    }

    /**
     * Migrate data in the registry db from H2 to Oracle
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     */
    private void migrateRegistryDatabase(Connection h2Connection, Connection oracleConnection) throws SQLException {
        RegistryDBMigration registryDBMigration = new RegistryDBMigration();
        registryDBMigration.selectAndInsertRegClusterLock(h2Connection, oracleConnection);
        System.out.println("migrated REG_CLUSTER_LOCK");
        registryDBMigration.selectAndInsertRegLog(h2Connection, oracleConnection);
        System.out.println("migrated REG_LOG");
        registryDBMigration.selectAndInsertRegPath(h2Connection, oracleConnection);
        System.out.println("migrated REG_PATH");
        registryDBMigration.selectAndInsertRegContent(h2Connection, oracleConnection);
        System.out.println("migrated REG_CONTENT");
        registryDBMigration.selectAndInsertRegContentHistory(h2Connection, oracleConnection);
        System.out.println("migrated REG_CONTENT_HISTORY");
        registryDBMigration.selectAndInsertRegResource(h2Connection, oracleConnection);
        System.out.println("migrated REG_RESOURCE");
        registryDBMigration.selectAndInsertRegResourceHistory(h2Connection, oracleConnection);
        System.out.println("migrated REG_RESOURCE_HISTORY");
        registryDBMigration.selectAndInsertRegComment(h2Connection, oracleConnection);
        System.out.println("migrated REG_COMMENT");
        registryDBMigration.selectAndInsertRegResourceComment(h2Connection, oracleConnection);
        System.out.println("migrated REG_RESOURCE_COMMENT");
        registryDBMigration.selectAndInsertRegRating(h2Connection, oracleConnection);
        System.out.println("migrated REG_RATING");
        registryDBMigration.selectAndInsertRegResourceRating(h2Connection, oracleConnection);
        System.out.println("migrated REG_RESOURCE_RATING");
        registryDBMigration.selectAndInsertRegTag(h2Connection, oracleConnection);
        System.out.println("migrated REG_TAG");
        registryDBMigration.selectAndInsertRegResourceTag(h2Connection, oracleConnection);
        System.out.println("migrated REG_RESOURCE_TAG");
        registryDBMigration.selectAndInsertRegProperty(h2Connection, oracleConnection);
        System.out.println("migrated REG_PROPERTY");
        registryDBMigration.selectAndInsertRegResourceProperty(h2Connection, oracleConnection);
        System.out.println("migrated REG_RESOURCE_PROPERTY");
        registryDBMigration.selectAndInsertRegAssociation(h2Connection, oracleConnection);
        System.out.println("migrated REG_ASSOCIATION");
        registryDBMigration.selectAndInsertRegSnapshot(h2Connection, oracleConnection);
        System.out.println("migrated REG_SNAPSHOT");

    }

    /**
     * Migrate data in the user manager db from H2 to Oracle
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     */
    private void migrateUserManagerDatabase(Connection h2Connection, Connection oracleConnection) throws SQLException {
        UserManagerDBMigration userManagerDBMigration = new UserManagerDBMigration();
        userManagerDBMigration.selectAndInsertUMTenant(h2Connection, oracleConnection);
        System.out.println("migrated UM_TENANT");
        userManagerDBMigration.selectAndInsertUMUser(h2Connection, oracleConnection);
        System.out.println("migrated UM_USER");
        userManagerDBMigration.selectAndInsertUMUserAttribute(h2Connection, oracleConnection);
        System.out.println("migrated UM_USER_ATTRIBUTE");
        userManagerDBMigration.selectAndInsertUMRole(h2Connection, oracleConnection);
        System.out.println("migrated UM_ROLE");
        userManagerDBMigration.selectAndInsertUMPermission(h2Connection, oracleConnection);
        System.out.println("migrated UM_PERMISSION");
        userManagerDBMigration.selectAndInsertUMRolePermission(h2Connection, oracleConnection);
        System.out.println("migrated UM_ROLE_PERMISSION");
        userManagerDBMigration.selectAndInsertUMUserPermission(h2Connection, oracleConnection);
        System.out.println("migrated UM_USER_PERMISSION");
        userManagerDBMigration.selectAndInsertUMUserRole(h2Connection, oracleConnection);
        System.out.println("migrated UM_USER_ROLE");
        userManagerDBMigration.selectAndInsertUMDialect(h2Connection, oracleConnection);
        System.out.println("migrated UM_DIALECT");
        userManagerDBMigration.selectAndInsertUMClaim(h2Connection, oracleConnection);
        System.out.println("migrated UM_CLAIM");
        userManagerDBMigration.selectAndInsertUMProfileConfig(h2Connection, oracleConnection);
        System.out.println("migrated UM_PROFILE_CONFIG");
        userManagerDBMigration.selectAndInsertUMClaimBehavior(h2Connection, oracleConnection);
        System.out.println("migrated UM_CLAIM_BEHAVIOR");
        userManagerDBMigration.selectAndInsertUMHybridRole(h2Connection, oracleConnection);
        System.out.println("migrated UM_HYBRID_ROLE");
        userManagerDBMigration.selectAndInsertUMHybridUserRole(h2Connection, oracleConnection);
        System.out.println("migrated UM_HYBRID_USER_ROLE");
        userManagerDBMigration.selectAndInsertUMHybridRememberMe(h2Connection, oracleConnection);
        System.out.println("migrated UM_HYBRID_REMEMBER_ME");

    }

}
