package com.wso2.support;

import java.sql.SQLException;

/**
 * Executable class of H2 to Oracle data migrate client
 */
public class Main {

    private String h2ConnectionURL = "jdbc:h2:/home/indika/wso2esb-4.0.0/repository/database/WSO2CARBON_DB;DB_CLOSE_ON_EXIT=FALSE";
    private String h2Username = "wso2carbon";
    private String h2Password = "wso2carbon";
    private String oracleConnectionURL = "jdbc:oracle:thin:@127.0.0.1:1521/ora11g";
    private String oracleUsername = "wso2carbon";
    private String oraclePassword = "wso2carbon";

    /**
     * Main execution class
     * @param args argument array to do registry copy
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        if (args != null && args.length == 6) {
            String h2ConnectionURL = args[0];
            String h2Username = args[1];
            String h2Password = args[2];
            String oracleConnectionURL = args[3];
            String oracleUsername = args[4];
            String oraclePassword = args[5];
            validateCommandLineArguments(h2ConnectionURL, h2Username, h2Password, oracleConnectionURL,
                    oracleUsername, oraclePassword);
            H2ToOracleMigrateClient h2ToOracleMigrateClient = new H2ToOracleMigrateClient();
            h2ToOracleMigrateClient.migrate(h2ConnectionURL, h2Username, h2Password, oracleConnectionURL,
                    oracleUsername, oraclePassword);
            System.out.println("Done!");
            System.exit(0);
        }else {
            throw new RuntimeException("Command line arguments count not matched. Please pass these arguments in " +
                    "respective order: 'H2DatabaseConnectionURL' 'H2DatabaseUsername' 'H2DatabasePassword' " +
                    "'OracleDatabaseConnectionURL' 'OracleDatabaseUsername' 'OracleDatabasePassword'");
        }
    }

    /**
     * Validate command line argument
     *
     * @param h2ConnectionURL H2 database connection url
     * @param h2Username H2 database username
     * @param h2Password H2 database password
     * @param oracleConnectionURL Oracle database connection url
     * @param oracleUsername Oracle database username
     * @param oraclePassword Oracle database password
     */
    private static void validateCommandLineArguments(String h2ConnectionURL, String h2Username, String h2Password, String
            oracleConnectionURL, String oracleUsername, String oraclePassword) {
        if (h2ConnectionURL.isEmpty()) {
            throw new RuntimeException("H2 database connection URL cannot be empty.");
        }
        if (h2Username.isEmpty()) {
            throw new RuntimeException("H2 database username cannot be empty.");
        }
        if (h2Password.isEmpty()) {
            throw new RuntimeException("H2 database password cannot be empty.");
        }
        if (oracleConnectionURL.isEmpty()) {
            throw new RuntimeException("Oracle database connection URL cannot be empty.");
        }
        if (oracleUsername.isEmpty()) {
            throw new RuntimeException("Oracle database username cannot be empty.");
        }
        if (oraclePassword.isEmpty()) {
            throw new RuntimeException("Oracle database password cannot be empty.");
        }
    }
}
