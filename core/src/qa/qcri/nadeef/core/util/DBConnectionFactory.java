/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.jooq.SQLDialect;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * Creates DB connection.
 */
public class DBConnectionFactory {

    // <editor-fold desc="Public methods">
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
     * Creates a new JDBC connection on the Nadeef database.
     * @return new JDBC connection.
     */
    public static Connection createNadeefConnection()
            throws
            ClassNotFoundException,
            SQLException,
            InstantiationException,
            IllegalAccessException {
        String url = NadeefConfiguration.getUrl();
        StringBuilder jdbcUrl = new StringBuilder("jdbc:");
        String type = NadeefConfiguration.getType();
        SQLDialect dialect;
        switch (type) {
            default:
            case "postgres":
                jdbcUrl.append("postgresql");
                dialect = SQLDialect.POSTGRES;
        }

        jdbcUrl.append("://");
        jdbcUrl.append(url);

        return createConnection(
                dialect,
                jdbcUrl.toString(),
                NadeefConfiguration.getUserName(),
                NadeefConfiguration.getPassword()
        );
    }

    /**
     * Creates a new JDBC connection on the source DB from a clean plan.
     * @param plan
     * @return new JDBC connection.
     */
    public static Connection createSourceConnection(CleanPlan plan)
            throws
                ClassNotFoundException,
                SQLException,
                InstantiationException,
                IllegalAccessException {
        String url = plan.getSourceUrl();
        StringBuilder jdbcUrl = new StringBuilder("jdbc:");
        SQLDialect dialect = plan.getSqlDialect();
        switch (dialect) {
            case POSTGRES:
                jdbcUrl.append("postgresql");
        }

        jdbcUrl.append("://");
        jdbcUrl.append(plan.getSourceUrl());

        return createConnection(
                plan.getSqlDialect(),
                jdbcUrl.toString(),
                plan.getSourceUserName(),
                plan.getSourceUserPassword()
        );
    }

    /**
     * Creates a new JDBC connection on the target DB from a clean plan.
     * @param plan clean plan.
     * @return new JDBC connection.
     */
    public static Connection createTargetConnection(CleanPlan plan)
            throws
            ClassNotFoundException,
            SQLException,
            InstantiationException,
            IllegalAccessException {
        return createSourceConnection(plan);
    }

    // </editor-fold>

    private static String getDriverName(SQLDialect dialect) {
        switch (dialect) {
            case POSTGRES:
                return "org.postgresql.Driver";
            default:
                throw new NotImplementedException();
        }
    }
}
