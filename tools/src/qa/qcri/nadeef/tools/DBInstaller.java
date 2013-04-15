/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.util.Tracer;

import java.sql.*;

/**
 * Installation helper for Nadeef DB.
 */
public class DBInstaller {
    /**
     * Check whether Nadeef is installed in the targeted database connection.
     * @param conn JDBC connection.
     * @param configuration configuration class.
     * @return TRUE when Nadeef is already installed on the database.
     */
    public static boolean isInstalled(Connection conn, NadeefConfiguration configuration)
            throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        String tableName = NadeefConfiguration.getViolationTableName();
        ResultSet resultSet = metaData.getTables(null, null, tableName, null);

        if (resultSet.next()) {
            return true;
        }
        return false;
    }

    /**
     * Install Nadeef in the target database.
     */
    public static void install(Connection conn, NadeefConfiguration configuration)
            throws SQLException {
        Tracer tracer = Tracer.getTracer(DBInstaller.class);
        if (isInstalled(conn, configuration)) {
            tracer.info("Nadeef is installed on the database, please try uninstall first.");
            return;
        }

        Statement stat = conn.createStatement();
        // stat.execute("CREATE SCHEMA " + configuration.getSchemaName());
        stat.execute(
                "CREATE TABLE " + configuration.getSchemaName() + "." +
                configuration.getViolationTableName() + " (" +
                "rid varchar(255), " +
                "tablename varchar(63), " +
                "tupleid int, " +
                "attribute varchar(63), value text)"
        );
        conn.commit();
    }

    /**
     * Uninstall Nadeef from the target database.
     * @param conn JDBC Connection.
     */
    public static void uninstall(Connection conn, NadeefConfiguration configuration)
            throws SQLException {
        if (isInstalled(conn, configuration)) {
            Statement stat = conn.createStatement();
            stat.execute("DROP SCHEMA " + configuration.getSchemaName() + " CASCADE");
            stat.close();
            conn.commit();
        }
    }
}
