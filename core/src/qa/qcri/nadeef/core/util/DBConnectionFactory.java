/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means â€œCleanâ€� in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.dbcp.BasicDataSource;
import org.postgresql.ds.PGPoolingDataSource;

import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.SQLDialect;
import qa.qcri.nadeef.tools.Tracer;

import com.google.common.base.Preconditions;

/**
 * Database JDBC Connection Factory class.
 */
public class DBConnectionFactory {
	
	private static final int INITIAL_CONNECTION = 5;
    private static final int MAX_CONNECTION = 20;
    private static PGPoolingDataSource nadeefPool;
    private static BasicDataSource sourcePool;
    private static DBConfig dbConfig;
    private static Tracer tracer = Tracer.getTracer(DBConnectionFactory.class);

    
    
    // <editor-fold desc="Public methods">

    /**
     * Initialize NADEEF database connection pool.
     */
    public static synchronized void initializeNadeefConnectionPool() {
        if (nadeefPool != null) {
            return;
        }

        DBConfig sourceConfig = NadeefConfiguration.getDbConfig();
        nadeefPool = new PGPoolingDataSource();
        nadeefPool.setDataSourceName("nadeef pool");
        nadeefPool.setDatabaseName(sourceConfig.getDatabaseName());
        nadeefPool.setServerName(sourceConfig.getServerName());
        nadeefPool.setUser(sourceConfig.getUserName());
        nadeefPool.setPassword(sourceConfig.getPassword());
        nadeefPool.setMaxConnections(MAX_CONNECTION);
    }

    /**
     * Shutdown the connection pool.
     */
    public synchronized static void shutdown() {
        if (sourcePool != null) {
            try {
				sourcePool.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }

        if (nadeefPool != null) {
            nadeefPool.close();
        }
        sourcePool = null;
        nadeefPool = null;
    }

    
    /**
     * Initialize the source database connection pool.
     * @param sourceConfig source config.
     */
    public synchronized static void initializeSource(DBConfig sourceConfig) {
        Preconditions.checkNotNull(sourceConfig);
        if (dbConfig == sourceConfig || (dbConfig != null && dbConfig.equals(sourceConfig))) {
            return;
        }

        if (sourcePool != null) {
            try {
				sourcePool.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
        }

        dbConfig = sourceConfig;
        sourcePool = new BasicDataSource();
        sourcePool.setUrl(sourceConfig.getUrl());
        sourcePool.setDriverClassName(CommonTools.getDriveClass(sourceConfig.getDialect()));
        sourcePool.setUsername(sourceConfig.getUserName());
        sourcePool.setPassword(sourceConfig.getPassword());
        sourcePool.setInitialSize(INITIAL_CONNECTION);
        sourcePool.setMaxActive(MAX_CONNECTION);
        sourcePool.setMaxIdle(MAX_CONNECTION);
    }

    /**
     * Creates a new JDBC connection on the Nadeef database.
     * @return new JDBC connection.
     */
    public static Connection getNadeefConnection() throws SQLException {
        Connection conn = nadeefPool.getConnection();
        conn.setAutoCommit(false);
        return conn;
    }

    /**
     * Creates a new JDBC connection on the source DB from a clean plan.
     * @return new JDBC connection.
     */
    public static Connection getSourceConnection() throws SQLException {
        Connection conn = sourcePool.getConnection();
        conn.setAutoCommit(false);
        return conn;
    }

    /**
     * Gets the source <code>DBConfig</code>.
     * @return <code>DBConfig</code>.
     */
    public static DBConfig getSourceDBConfig() {
        return dbConfig;
    }

    // </editor-fold>

    //<editor-fold desc="Private methods">
    private static String getDriverName(SQLDialect dialect) {
        switch (dialect) {
            case POSTGRES:
                return "org.postgresql.Driver";
            case MYSQL:
            	return "com.mysql.jdbc.Driver";
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Get the JDBC connection based on the dialect.
     * @param dialect Database type.
     * @param url Database URL.
     * @param userName login user name.
     * @param password Login user password.
     * @depreciated
     * @return JDBC connection.
     */
    public static Connection createConnection(
        SQLDialect dialect,
        String url,
        String userName,
        String password
    ) throws ClassNotFoundException, SQLException, IllegalAccessException, InstantiationException {
        String driverName = getDriverName(dialect);
        Class.forName(driverName).newInstance();
        Connection conn = DriverManager.getConnection(url, userName, password);
        conn.setAutoCommit(false);
        return conn;
    }

    /**
     * Get the JDBC connection based on the dialect.
     * @param dbConfig dbconfig.
     * @depreciated
     * @return JDBC connection.
     */
    public static Connection createConnection(DBConfig dbConfig)
        throws
            ClassNotFoundException,
            SQLException,
            IllegalAccessException,
            InstantiationException {
        String driverName = getDriverName(dbConfig.getDialect());
        Class.forName(driverName).newInstance();
        Connection conn =
            DriverManager.getConnection(
                dbConfig.getUrl(),
                dbConfig.getUserName(),
                dbConfig.getPassword()
            );
        conn.setAutoCommit(false);
        return conn;
    }

    //</editor-fold>
}
