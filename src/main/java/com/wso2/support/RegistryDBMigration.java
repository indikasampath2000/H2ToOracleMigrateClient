package com.wso2.support;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * This class responsible for migrate data in registry related tables. Make sure to execute all method in this class
 * top to bottom to avoid foreign key violations.
 */
public class RegistryDBMigration {

    /**
     * Migrate data in REG_CLUSTER_LOCK table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegClusterLock(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_CLUSTER_LOCK);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_CLUSTER_LOCK);
            while (resultSet.next()) {
                preparedStatement.setString(1, resultSet.getString("REG_LOCK_NAME"));
                preparedStatement.setString(2, resultSet.getString("REG_LOCK_STATUS"));
                preparedStatement.setTimestamp(3, resultSet.getTimestamp("REG_LOCKED_TIME"));
                preparedStatement.setInt(4, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_LOG table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegLog(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_LOG);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_LOG);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_LOG_ID"));
                preparedStatement.setString(2, resultSet.getString("REG_PATH"));
                preparedStatement.setString(3, resultSet.getString("REG_USER_ID"));
                preparedStatement.setTimestamp(4, resultSet.getTimestamp("REG_LOGGED_TIME"));
                preparedStatement.setInt(5, resultSet.getInt("REG_ACTION"));
                preparedStatement.setString(6, resultSet.getString("REG_ACTION_DATA"));
                preparedStatement.setInt(7, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_PATH table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegPath(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_PATH);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_PATH);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_PATH_ID"));
                preparedStatement.setString(2, resultSet.getString("REG_PATH_VALUE"));
                preparedStatement.setInt(3, resultSet.getInt("REG_PATH_PARENT_ID"));
                preparedStatement.setInt(4, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_CONTENT table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegContent(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_CONTENT);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_CONTENT);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_CONTENT_ID"));
                preparedStatement.setBlob(2, resultSet.getBlob("REG_CONTENT_DATA"));
                preparedStatement.setInt(3, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_CONTENT_HISTORY table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegContentHistory(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_CONTENT_HISTORY);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_CONTENT_HISTORY);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_CONTENT_ID"));
                preparedStatement.setBlob(2, resultSet.getBlob("REG_CONTENT_DATA"));
                preparedStatement.setShort(3, resultSet.getShort("REG_DELETED"));
                preparedStatement.setInt(4, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_RESOURCE table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegResource(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_RESOURCE);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_RESOURCE);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_PATH_ID"));
                preparedStatement.setString(2, resultSet.getString("REG_NAME"));
                preparedStatement.setInt(3, resultSet.getInt("REG_VERSION"));
                preparedStatement.setString(4, resultSet.getString("REG_MEDIA_TYPE"));
                preparedStatement.setString(5, resultSet.getString("REG_CREATOR"));
                preparedStatement.setTimestamp(6, resultSet.getTimestamp("REG_CREATED_TIME"));
                preparedStatement.setString(7, resultSet.getString("REG_LAST_UPDATOR"));
                preparedStatement.setTimestamp(8, resultSet.getTimestamp("REG_LAST_UPDATED_TIME"));
                preparedStatement.setString(9, resultSet.getString("REG_DESCRIPTION"));
                preparedStatement.setInt(10, resultSet.getInt("REG_CONTENT_ID"));
                preparedStatement.setInt(11, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_RESOURCE_HISTORY table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegResourceHistory(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_RESOURCE_HISTORY);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_RESOURCE_HISTORY);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_PATH_ID"));
                preparedStatement.setString(2, resultSet.getString("REG_NAME"));
                preparedStatement.setInt(3, resultSet.getInt("REG_VERSION"));
                preparedStatement.setString(4, resultSet.getString("REG_MEDIA_TYPE"));
                preparedStatement.setString(5, resultSet.getString("REG_CREATOR"));
                preparedStatement.setTimestamp(6, resultSet.getTimestamp("REG_CREATED_TIME"));
                preparedStatement.setString(7, resultSet.getString("REG_LAST_UPDATOR"));
                preparedStatement.setTimestamp(8, resultSet.getTimestamp("REG_LAST_UPDATED_TIME"));
                preparedStatement.setString(9, resultSet.getString("REG_DESCRIPTION"));
                preparedStatement.setInt(10, resultSet.getInt("REG_CONTENT_ID"));
                preparedStatement.setShort(11, resultSet.getShort("REG_DELETED"));
                preparedStatement.setInt(12, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_COMMENT table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegComment(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_COMMENT);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_COMMENT);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_ID"));
                preparedStatement.setString(2, resultSet.getString("REG_COMMENT_TEXT"));
                preparedStatement.setString(3, resultSet.getString("REG_USER_ID"));
                preparedStatement.setTimestamp(4, resultSet.getTimestamp("REG_COMMENTED_TIME"));
                preparedStatement.setInt(5, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_RESOURCE_COMMENT table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegResourceComment(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_RESOURCE_COMMENT);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_RESOURCE_COMMENT);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_COMMENT_ID"));
                preparedStatement.setInt(2, resultSet.getInt("REG_VERSION"));
                preparedStatement.setInt(3, resultSet.getInt("REG_PATH_ID"));
                preparedStatement.setString(4, resultSet.getString("REG_RESOURCE_NAME"));
                preparedStatement.setInt(5, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_RATING table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegRating(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_RATING);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_RATING);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_ID"));
                preparedStatement.setInt(2, resultSet.getInt("REG_RATING"));
                preparedStatement.setString(3, resultSet.getString("REG_USER_ID"));
                preparedStatement.setTimestamp(4, resultSet.getTimestamp("REG_RATED_TIME"));
                preparedStatement.setInt(5, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_RESOURCE_RATING table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegResourceRating(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_RESOURCE_RATING);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_RESOURCE_RATING);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_RATING_ID"));
                preparedStatement.setInt(2, resultSet.getInt("REG_VERSION"));
                preparedStatement.setInt(3, resultSet.getInt("REG_PATH_ID"));
                preparedStatement.setString(4, resultSet.getString("REG_RESOURCE_NAME"));
                preparedStatement.setInt(5, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_TAG table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegTag(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_TAG);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_TAG);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_ID"));
                preparedStatement.setString(2, resultSet.getString("REG_TAG_NAME"));
                preparedStatement.setString(3, resultSet.getString("REG_USER_ID"));
                preparedStatement.setTimestamp(4, resultSet.getTimestamp("REG_TAGGED_TIME"));
                preparedStatement.setInt(5, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_RESOURCE_TAG table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegResourceTag(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_RESOURCE_TAG);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_RESOURCE_TAG);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_TAG_ID"));
                preparedStatement.setInt(2, resultSet.getInt("REG_VERSION"));
                preparedStatement.setInt(3, resultSet.getInt("REG_PATH_ID"));
                preparedStatement.setString(4, resultSet.getString("REG_RESOURCE_NAME"));
                preparedStatement.setInt(5, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_PROPERTY table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegProperty(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_PROPERTY);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_PROPERTY);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_ID"));
                preparedStatement.setString(2, resultSet.getString("REG_NAME"));
                preparedStatement.setString(3, resultSet.getString("REG_VALUE"));
                preparedStatement.setInt(4, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_RESOURCE_PROPERTY table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegResourceProperty(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_RESOURCE_PROPERTY);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_RESOURCE_PROPERTY);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_PROPERTY_ID"));
                preparedStatement.setInt(2, resultSet.getInt("REG_VERSION"));
                preparedStatement.setInt(3, resultSet.getInt("REG_PATH_ID"));
                preparedStatement.setString(4, resultSet.getString("REG_RESOURCE_NAME"));
                preparedStatement.setInt(5, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_ASSOCIATION table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegAssociation(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_ASSOCIATION);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_ASSOCIATION);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_ASSOCIATION_ID"));
                preparedStatement.setString(2, resultSet.getString("REG_SOURCEPATH"));
                preparedStatement.setString(3, resultSet.getString("REG_TARGETPATH"));
                preparedStatement.setString(4, resultSet.getString("REG_ASSOCIATION_TYPE"));
                preparedStatement.setInt(5, resultSet.getInt("REG_TENANT_ID"));
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
     * Migrate data in REG_SNAPSHOT table
     *
     * @param h2Connection H2 db connection
     * @param oracleConnection Oracle db connection
     * @throws SQLException
     */
    public void selectAndInsertRegSnapshot(Connection h2Connection, Connection oracleConnection) throws
            SQLException {
        Statement statement = null;
        ResultSet resultSet = null;
        PreparedStatement preparedStatement = null;
        try {
            statement = h2Connection.createStatement();
            resultSet = statement.executeQuery(MigrationClientConstant.SELECT_REG_SNAPSHOT);
            preparedStatement = oracleConnection.prepareStatement(MigrationClientConstant
                    .INSERT_REG_SNAPSHOT);
            while (resultSet.next()) {
                preparedStatement.setInt(1, resultSet.getInt("REG_SNAPSHOT_ID"));
                preparedStatement.setInt(2, resultSet.getInt("REG_PATH_ID"));
                preparedStatement.setString(3, resultSet.getString("REG_RESOURCE_NAME"));
                preparedStatement.setBlob(4, resultSet.getBlob("REG_RESOURCE_VIDS"));
                preparedStatement.setInt(5, resultSet.getInt("REG_TENANT_ID"));
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
