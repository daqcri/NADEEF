/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means “Clean” in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.util;

import com.google.common.base.Preconditions;
import org.postgresql.ds.PGPoolingDataSource;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.SQLDialect;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Database JDBC Connection Factory class.
 */
public class DBConnectionFactory {
    private static Tracer tracer = Tracer.getTracer(DBConnectionFactory.class);
    private static final int MAX_CONNECTION = Runtime.getRuntime().availableProcessors() * 2;
    private static PGPoolingDataSource nadeefPool;

    private PGPoolingDataSource sourcePool;
    private DBConfig sourceConfig;

    private DBConnectionFactory(DBConfig dbConfig) {
        tracer.info("Creating connection pool for " + dbConfig.getUrl());
        initializeSource(dbConfig);
    }

    // <editor-fold desc="Public methods">

    /**
     * Creates a connection pool factory.
     * @param dbConfig dbConfig.
     * @return connection factory instance.
     */
    public static DBConnectionFactory createDBConnectionFactory(DBConfig dbConfig) {
        return new DBConnectionFactory(dbConfig);
    }

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
        if (nadeefPool != null) {
            nadeefPool.close();
        }

        nadeefPool = null;
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
    public Connection getSourceConnection() throws SQLException {
        Connection conn = sourcePool.getConnection();
        conn.setAutoCommit(false);
        return conn;
    }

    /**
     * Gets the source <code>DBConfig</code>.
     * @return <code>DBConfig</code>.
     */
    public DBConfig getSourceDBConfig() {
        return sourceConfig;
    }

    /**
     * Gets the JDBC connection based on the dialect.
     * @param dialect Database type.
     * @param url Database URL.
     * @param userName login user name.
     * @param password Login user password.
     * @return JDBC connection.
     */
    public static Connection createConnection(
            SQLDialect dialect,
            String url,
            String userName,
            String password
    ) throws
        ClassNotFoundException,
        SQLException,
        IllegalAccessException,
        InstantiationException {
        String driverName = getDriverName(dialect);
        Class.forName(driverName).newInstance();
        Connection conn = DriverManager.getConnection(url, userName, password);
        conn.setAutoCommit(false);
        return conn;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finalize() {
        tracer.info("Closing a connection pool @" + sourcePool.getDataSourceName());
        if (sourcePool != null) {
            sourcePool.close();
        }
        sourcePool = null;
    }

    // </editor-fold>

    //<editor-fold desc="Private methods">
    private static String getDriverName(SQLDialect dialect) {
        switch (dialect) {
            case POSTGRES:
                return "org.postgresql.Driver";
            default:
                throw new UnsupportedOperationException();
        }
    }

    /**
     * Get the JDBC connection based on the dialect.
     * @param dbConfig dbconfig.
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

    /**
     * Initialize the source database connection pool.
     * @param dbConfig db source config.
     */
    private void initializeSource(DBConfig dbConfig) {
        if (dbConfig == sourceConfig || (dbConfig != null && dbConfig.equals(sourceConfig))) {
            return;
        }

        if (sourcePool != null) {
            sourcePool.close();
        }

        sourceConfig = dbConfig;
        sourcePool = new PGPoolingDataSource();
        sourcePool.setDataSourceName(Long.toString(System.currentTimeMillis()));
        sourcePool.setDatabaseName(sourceConfig.getDatabaseName());
        sourcePool.setServerName(sourceConfig.getServerName());
        sourcePool.setUser(sourceConfig.getUserName());
        sourcePool.setPassword(sourceConfig.getPassword());
        sourcePool.setMaxConnections(MAX_CONNECTION);
    }

    //</editor-fold>
}
