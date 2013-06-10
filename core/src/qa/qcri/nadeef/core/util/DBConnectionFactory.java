/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    private static final int MAX_CONNECTION = 20;
    private static PGPoolingDataSource nadeefPool;
    private static PGPoolingDataSource sourcePool;
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
            sourcePool.close();
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
            sourcePool.close();
        }

        dbConfig = sourceConfig;
        sourcePool = new PGPoolingDataSource();
        sourcePool.setDataSourceName("source pool");
        sourcePool.setDatabaseName(sourceConfig.getDatabaseName());
        sourcePool.setServerName(sourceConfig.getServerName());
        sourcePool.setUser(sourceConfig.getUserName());
        sourcePool.setPassword(sourceConfig.getPassword());
        sourcePool.setMaxConnections(MAX_CONNECTION);
    }

    /**
     * Creates a new JDBC connection on the Nadeef database.
     * @return new JDBC connection.
     */
    public static synchronized Connection getNadeefConnection() throws SQLException {
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
