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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.dbcp.BasicDataSource;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;
import qa.qcri.nadeef.tools.sql.SQLDialectTools;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Database JDBC Connection Factory class.
 */
public class DBConnectionPool {
    private static Tracer tracer = Tracer.getTracer(DBConnectionPool.class);
    private static final int MAX_ACTIVE = Runtime.getRuntime().availableProcessors() * 2;

    private BasicDataSource nadeefPool;
    private BasicDataSource sourcePool;
    private DBConfig sourceConfig;
    private DBConfig nadeefConfig;

    private DBConnectionPool(DBConfig sourceConfig, DBConfig nadeefConfig) {
        this.sourceConfig = Preconditions.checkNotNull(sourceConfig);
        this.nadeefConfig = Preconditions.checkNotNull(nadeefConfig);

        nadeefPool = createConnectionPool(nadeefConfig);
        sourcePool = createConnectionPool(sourceConfig);
    }

    // <editor-fold desc="Public methods">

    /**
     * Creates a connection pool factory.
     * @param source source dbConfig.
     * @param nadeefConfig NADEEF dbConfig.
     * @return connection pool instance.
     */
    public static DBConnectionPool createDBConnectionPool(
        DBConfig source,
        DBConfig nadeefConfig
    ) {
        return new DBConnectionPool(source, nadeefConfig);
    }

    /**
     * Create a connection pool.
     * @param dbconfig input DB config.
     * @return connection pool instance.
     */
    public BasicDataSource createConnectionPool(DBConfig dbconfig) {
        tracer.verbose("Creating connection pool for " + dbconfig.getUrl());
        BasicDataSource result;
        result = new BasicDataSource();
        result.setUrl(dbconfig.getUrl());
        result.setDriverClassName(SQLDialectTools.getDriverName(dbconfig.getDialect()));

        String username = dbconfig.getUserName();
        if (!Strings.isNullOrEmpty(username)) {
            result.setUsername(username);
        }

        String password = dbconfig.getPassword();
        if (!Strings.isNullOrEmpty(password)) {
            result.setPassword(password);
        }

        result.setMaxActive(MAX_ACTIVE);
        result.setMaxIdle(MAX_ACTIVE * 3);
        result.setDefaultAutoCommit(false);
        return result;
    }

    /**
     * Shutdown the connection pool.
     */
    public void shutdown() {
        try {
            if (nadeefPool != null) {
                nadeefPool.close();
                tracer.verbose("Closing a connection pool @" + nadeefPool.getUrl());
            }

            if (sourcePool != null) {
                sourcePool.close();
                tracer.verbose("Closing a connection pool @" + sourcePool.getUrl());
            }
        } catch (Exception e) {
            tracer.err("Exception during closing NADEEF pool.", e);
        }
    }

    /**
     * Creates a new JDBC connection on the Nadeef database.
     * @return new JDBC connection.
     */
    public Connection getNadeefConnection() throws SQLException {
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
     * Gets the source {@link DBConfig}.
     * @return {@link DBConfig}.
     */
    public DBConfig getSourceDBConfig() {
        return sourceConfig;
    }

    /**
     * Gets the NADEEF {@link DBConfig}.
     * @return {@link DBConfig}.
     */
    public DBConfig getNadeefConfig() {
        return nadeefConfig;
    }

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
     * Gets the JDBC connection based on the dialect.
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
    //</editor-fold>
}
