/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import java.sql.*;

/**
 * Installation helper for Nadeef DB.
 */
public class DBInstaller {
    /**
     * Check whether Nadeef is installed in the targeted database connection.
     * @param conn JDBC connection.
     * @param tableName source tableName.
     * @return TRUE when Nadeef is already installed on the database.
     */
    public static boolean isInstalled(Connection conn, String tableName)
            throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet resultSet = metaData.getTables(null, null, tableName, null);

        if (resultSet.next()) {
            return true;
        }
        return false;
    }

    /**
     * Install Nadeef in the target database.
     */
    // TODO: move connection inside the method.
    public static void install(Connection conn, String tableName)
            throws SQLException {
        Tracer tracer = Tracer.getTracer(DBInstaller.class);
        if (isInstalled(conn, tableName)) {
            tracer.info("Nadeef is installed on the database, please try uninstall first.");
            return;
        }

        Statement stat = conn.createStatement();
        // TODO: make an index key for violations
        stat.execute(
                "CREATE TABLE " +
                tableName + " (" +
                "vid int," +
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
    public static void uninstall(Connection conn, String tableName)
            throws SQLException {
        if (isInstalled(conn, tableName)) {
            Statement stat = conn.createStatement();
            stat.execute("DROP TABLE " + tableName + " CASCADE");
            stat.close();
            conn.commit();
        }
    }
}
