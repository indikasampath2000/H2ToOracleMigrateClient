package com.wso2.support;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This class responsible for migrate data in user manager related tables. Make sure to execute all method in this class
 * top to bottom to avoid foreign key violations.
 */
public class UserManagerDBMigration {

    /**
     * Migrate data in UM_TENANT table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMTenant(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_TENANT);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_TENANT);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setString(2, resultSet.getString("UM_DOMAIN_NAME"));
                preparedStatement.setString(3, resultSet.getString("UM_EMAIL"));
                preparedStatement.setBoolean(4, resultSet.getBoolean("UM_ACTIVE"));
                preparedStatement.setTimestamp(5, resultSet.getTimestamp("UM_CREATED_DATE"));
                preparedStatement.setBlob(6, resultSet.getBlob("UM_USER_CONFIG"));
                preparedStatement.executeUpdate();
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    /**
     * Migrate data in UM_USER table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMUser(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_USER);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_USER);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setString(2, resultSet.getString("UM_USER_NAME"));
                preparedStatement.setString(3, resultSet.getString("UM_USER_PASSWORD"));
                preparedStatement.setString(4, resultSet.getString("UM_SALT_VALUE"));
                preparedStatement.setBoolean(5, resultSet.getBoolean("UM_REQUIRE_CHANGE"));
                preparedStatement.setTimestamp(6, resultSet.getTimestamp("UM_CHANGED_TIME"));
                preparedStatement.setInt(7, resultSet.getInt("UM_TENANT_ID"));
                preparedStatement.executeUpdate();
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    /**
     * Migrate data in UM_USER_ATTRIBUTE table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMUserAttribute(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_USER_ATTRIBUTE);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_USER_ATTRIBUTE);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setString(2, resultSet.getString("UM_ATTR_NAME"));
                preparedStatement.setString(3, resultSet.getString("UM_ATTR_VALUE"));
                preparedStatement.setString(4, resultSet.getString("UM_PROFILE_ID"));
                preparedStatement.setInt(5, resultSet.getInt("UM_USER_ID"));
                preparedStatement.setInt(6, resultSet.getInt("UM_TENANT_ID"));
                preparedStatement.executeUpdate();
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    /**
     * Migrate data in UM_ROLE table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMRole(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_ROLE);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_ROLE);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setString(2, resultSet.getString("UM_ROLE_NAME"));
                preparedStatement.setInt(3, resultSet.getInt("UM_TENANT_ID"));
                preparedStatement.executeUpdate();
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }

    /**
     * Migrate data in UM_PERMISSION table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMPermission(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_PERMISSION);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_PERMISSION);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setString(2, resultSet.getString("UM_RESOURCE_ID"));
                preparedStatement.setString(3, resultSet.getString("UM_ACTION"));
                preparedStatement.setInt(4, resultSet.getInt("UM_TENANT_ID"));
                preparedStatement.executeUpdate();
            }
        } finally {
            if (statement != null) {
                statement.close();
            }
            if (resultSet != null) {
                resultSet.close();
            }
            if (preparedStatement != null) {
                preparedStatement.close();
            }
        }
    }
}
