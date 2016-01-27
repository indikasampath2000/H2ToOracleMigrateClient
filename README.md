# H2ToOracleMigrateClient

Supported java version 1.7

H2 to Oracle data migration client written since there is no direct data migration from two different databases.
Please note that client support only carbon 3.2.0 version i.e. data migration can be done between H2 to Oracle in
carbon 3.2.0 version and you cannot migrate data from carbon 3.2.0 version H2 database to carbon 4.2.0 version Oracle
database. Please follow up below instructions inorder to execute client.

1. H2 database has file locking mechanism. You cannot run this client when carbon server up and running. Therefore,
first you need to shut down the carbon server. Also make sure to keep back of existing H2 database file. For an
example, you can find H2 database in wso2esb-4.0.0/repository/database/ folder.

2. Create Oracle database and run oracle.sql. For an example, you can find oracle.sql in wso2esb-4.0.0/dbscripts
folder.

3. Next you can execute client passing below set of command line arguments respectively.
H2DatabaseConnectionURL - H2 database connection URL with fully qualified path tp H2 database file
H2DatabaseUsername - H2 database username
H2DatabasePassword - H2 database password
OracleDatabaseConnectionURL - Oracle database connection URL
OracleDatabaseUsername - Oracle database username
OracleDatabasePassword - Oracle database password

Once you build the project, executable jar is h2-to-oracle-migrate-client-1.0-SNAPSHOT.jar. You can run it with below
command. Please execute jar keep in the same distribution folder without copying to any other location because there
are some dependencies and resources it required at runtime.

java -jar h2-to-oracle-migrate-client-1.0-SNAPSHOT.jar 'jdbc:h2:/home/indika/wso2esb-4.0.0/repository/database/WSO2CARBON_DB;DB_CLOSE_ON_EXIT=FALSE' 'wso2carbon' 'wso2carbon' 'jdbc:oracle:thin:@127.0.0.1:1521/ora11g' 'wso2carbon' 'wso2carbon'