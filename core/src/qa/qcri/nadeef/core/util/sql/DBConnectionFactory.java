/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.util.sql;

import com.google.common.base.Strings;
import org.apache.commons.dbcp.BasicDataSource;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;
import qa.qcri.nadeef.tools.sql.SQLDialectTools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Database JDBC Connection Factory class.
 *
 * @author Si Yin <siyin@qf.org.qa>
 */
public class DBConnectionFactory {
    private static Tracer tracer = Tracer.getTracer(DBConnectionFactory.class);
    private static final int MAX_ACTIVE = Runtime.getRuntime().availableProcessors() * 2;
    private static BasicDataSource nadeefPool;

    private BasicDataSource sourcePool;
    private DBConfig sourceConfig;

    private DBConnectionFactory(DBConfig dbConfig) {
        tracer.verbose("Creating connection pool for " + dbConfig.getUrl());
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

        // special start for derby server
        DBConfig dbconfig = NadeefConfiguration.getDbConfig();
        nadeefPool = new BasicDataSource();
        nadeefPool.setUrl(dbconfig.getUrl());
        nadeefPool.setDriverClassName(SQLDialectTools.getDriverName(dbconfig.getDialect()));

        String username = dbconfig.getUserName();
        if (!Strings.isNullOrEmpty(username)) {
            nadeefPool.setUsername(username);
        }

        String password = dbconfig.getPassword();
        if (!Strings.isNullOrEmpty(password)) {
            nadeefPool.setPassword(password);
        }

        nadeefPool.setMaxActive(MAX_ACTIVE);
        nadeefPool.setMaxIdle(MAX_ACTIVE * 3);
        nadeefPool.setDefaultAutoCommit(false);
    }

    /**
     * Shutdown the connection pool.
     */
    public synchronized static void shutdown() {
        if (nadeefPool != null) {
            try {
                nadeefPool.close();
            } catch (Exception e) {
                tracer.err("Exception during closing NADEEF pool.", e);
            }
        }

        nadeefPool = null;
    }

    /**
     * Creates a new JDBC connection on the Nadeef database.
     * @return new JDBC connection.
     */
    public static Connection getNadeefConnection() throws SQLException {
        return nadeefPool.getConnection();
    }

    /**
     * Creates a new JDBC connection on the source DB from a clean plan.
     * @return new JDBC connection.
     */
    public Connection getSourceConnection() throws SQLException {
        return sourcePool.getConnection();
    }

    /**
     * Gets the source <code>DBConfig</code>.
     * @return <code>DBConfig</code>.
     */
    public DBConfig getSourceDBConfig() {
        return sourceConfig;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void finalize() throws Throwable {
        // skip finalizing the nadeef pool
        if (sourcePool == nadeefPool) {
            return;
        }

        tracer.verbose("Closing a connection pool @" + sourcePool.getUrl());

        try {
            sourcePool.close();
        } catch (Exception e) {
            tracer.err("Exception during closing source pool", e);
        }

       super.finalize();
    }

    // </editor-fold>

    /**
     * Gets the JDBC connection based on the dialect.
     * @param dbConfig dbconfig.
     * @param autoCommit auto commit flag.
     * @return JDBC connection.
     */
    public static Connection createConnection(DBConfig dbConfig, boolean autoCommit)
        throws
            ClassNotFoundException,
            SQLException,
            IllegalAccessException,
            InstantiationException {
        String driverName = SQLDialectTools.getDriverName(dbConfig.getDialect());
        Class.forName(driverName).newInstance();
        Connection conn =
            DriverManager.getConnection(
                dbConfig.getUrl(),
                dbConfig.getUserName(),
                dbConfig.getPassword()
            );
        conn.setAutoCommit(autoCommit);
        return conn;
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
        return createConnection(dbConfig, false);
    }

    /**
     * Initialize the source database connection pool.
     * @param dbConfig db source config.
     */
    // TODO: currently it is limited to 1 source db.
    private void initializeSource(DBConfig dbConfig) {
        if (dbConfig == sourceConfig || dbConfig.equals(sourceConfig)) {
            return;
        }

        // if the source is as the same as the nadeef source,
        // reuse the nadeef connection pool.
        DBConfig nadeefConfig = NadeefConfiguration.getDbConfig();
        if (dbConfig == nadeefConfig ||
            dbConfig.getUrl().equalsIgnoreCase(nadeefConfig.getUrl())) {
            sourcePool = nadeefPool;
            sourceConfig = dbConfig;
            return;
        }

        if (sourcePool != null) {
            try {
                sourcePool.close();
            } catch (Exception ex) {
                tracer.err("Exception during closing source pool", ex);
            }
        }

        sourceConfig = dbConfig;
        sourcePool = new BasicDataSource();
        sourcePool.setUrl(sourceConfig.getUrl());
        sourcePool.setDriverClassName(SQLDialectTools.getDriverName(sourceConfig.getDialect()));

        String username = sourceConfig.getUserName();
        if (!Strings.isNullOrEmpty(username)) {
            sourcePool.setUsername(sourceConfig.getUserName());
        }

        String password = sourceConfig.getPassword();
        if (!Strings.isNullOrEmpty(password)) {
            sourcePool.setPassword(password);
        }

        sourcePool.setMaxActive(MAX_ACTIVE);
        sourcePool.setMaxIdle(MAX_ACTIVE * 3);
        sourcePool.setDefaultAutoCommit(false);
    }

    //</editor-fold>
}
