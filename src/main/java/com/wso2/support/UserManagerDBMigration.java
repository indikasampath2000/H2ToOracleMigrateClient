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
                preparedStatement.setInt(7, -1234);
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
                preparedStatement.setInt(6, -1234);
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
                preparedStatement.setInt(3, -1234);
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
                String umResourceId = resultSet.getString("UM_RESOURCE_ID");
                //if resource id which is secured, then modify it to support by esb 4.8.1
                if (umResourceId.startsWith("/repository/axis2/service-groups")) {
                    umResourceId = getModifiedResourceId(umResourceId);
                }
                preparedStatement.setString(2, umResourceId);
                preparedStatement.setString(3, resultSet.getString("UM_ACTION"));
                preparedStatement.setInt(4, -1234);
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
     * Migrate data in UM_ROLE_PERMISSION table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMRolePermission(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_ROLE_PERMISSION);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_ROLE_PERMISSION);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setInt(2, resultSet.getInt("UM_PERMISSION_ID"));
                preparedStatement.setString(3, resultSet.getString("UM_ROLE_NAME"));
                preparedStatement.setShort(4, resultSet.getShort("UM_IS_ALLOWED"));
                preparedStatement.setInt(5, -1234);
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
     * Migrate data in UM_USER_PERMISSION table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMUserPermission(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_USER_PERMISSION);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_USER_PERMISSION);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setInt(2, resultSet.getInt("UM_PERMISSION_ID"));
                preparedStatement.setString(3, resultSet.getString("UM_USER_NAME"));
                preparedStatement.setShort(4, resultSet.getShort("UM_IS_ALLOWED"));
                preparedStatement.setInt(5, -1234);
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
     * Migrate data in UM_USER_ROLE table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMUserRole(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_USER_ROLE);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_USER_ROLE);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setInt(2, resultSet.getInt("UM_ROLE_ID"));
                preparedStatement.setInt(3, resultSet.getInt("UM_USER_ID"));
                preparedStatement.setInt(4, -1234);
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
     * Migrate data in UM_DIALECT table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMDialect(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_DIALECT);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_DIALECT);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setString(2, resultSet.getString("UM_DIALECT_URI"));
                preparedStatement.setInt(3, -1234);
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
     * Migrate data in UM_CLAIM table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMClaim(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_CLAIM);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_CLAIM);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setInt(2, resultSet.getInt("UM_DIALECT_ID"));
                preparedStatement.setString(3, resultSet.getString("UM_CLAIM_URI"));
                preparedStatement.setString(4, resultSet.getString("UM_DISPLAY_TAG"));
                preparedStatement.setString(5, resultSet.getString("UM_DESCRIPTION"));
                preparedStatement.setString(6, resultSet.getString("UM_MAPPED_ATTRIBUTE"));
                preparedStatement.setString(7, resultSet.getString("UM_REG_EX"));
                preparedStatement.setShort(8, resultSet.getShort("UM_SUPPORTED"));
                preparedStatement.setShort(9, resultSet.getShort("UM_REQUIRED"));
                preparedStatement.setInt(10, resultSet.getInt("UM_DISPLAY_ORDER"));
                preparedStatement.setInt(11, -1234);
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
     * Migrate data in UM_PROFILE_CONFIG table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMProfileConfig(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_PROFILE_CONFIG);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_PROFILE_CONFIG);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setInt(2, resultSet.getInt("UM_DIALECT_ID"));
                preparedStatement.setString(3, resultSet.getString("UM_PROFILE_NAME"));
                preparedStatement.setInt(4, -1234);
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
     * Migrate data in UM_CLAIM_BEHAVIOR table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMClaimBehavior(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_CLAIM_BEHAVIOR);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_CLAIM_BEHAVIOR);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setInt(2, resultSet.getInt("UM_PROFILE_ID"));
                preparedStatement.setInt(3, resultSet.getInt("UM_CLAIM_ID"));
                preparedStatement.setShort(4, resultSet.getShort("UM_BEHAVIOUR"));
                preparedStatement.setInt(5, -1234);
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
     * Migrate data in UM_HYBRID_ROLE table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMHybridRole(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_HYBRID_ROLE);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_HYBRID_ROLE);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setString(2, resultSet.getString("UM_ROLE_NAME"));
                preparedStatement.setInt(3, -1234);
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
     * Migrate data in UM_HYBRID_USER_ROLE table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMHybridUserRole(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_HYBRID_USER_ROLE);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_HYBRID_USER_ROLE);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setString(2, resultSet.getString("UM_USER_NAME"));
                preparedStatement.setInt(3, resultSet.getInt("UM_ROLE_ID"));
                preparedStatement.setInt(4, -1234);
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
     * Migrate data in UM_HYBRID_REMEMBER_ME table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertUMHybridRememberMe(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_UM_HYBRID_REMEMBER_ME);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_UM_HYBRID_REMEMBER_ME);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("UM_ID"));
                preparedStatement.setString(2, resultSet.getString("UM_USER_NAME"));
                preparedStatement.setString(3, resultSet.getString("UM_COOKIE_VALUE"));
                preparedStatement.setTimestamp(4, resultSet.getTimestamp("UM_CREATED_TIME"));
                preparedStatement.setInt(5, -1234);
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
     * um_resource_id format changed carbon 3.2.0 to carbon 4.2.0. Earlier it was resource path in config registry, but
     * now it's changed to serviceGroupId/serviceId. This method read existing um_resource_id and return modified
     * um_resource_id which is valid for carbon 4.2.0
     *
     * @param oldResourceId existing um_resource_id
     * @return modified um_resource_id
     */
    private String getModifiedResourceId(String oldResourceId) {
        String modifiedResourceId = oldResourceId.substring(oldResourceId.lastIndexOf("/") + 1);
        return modifiedResourceId.toLowerCase() + "/" + modifiedResourceId.toLowerCase();
    }
}
