/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import qa.qcri.nadeef.core.util.NadeefConfiguration;
import qa.qcri.nadeef.core.util.Tracer;

import java.sql.*;

/**
 * Installation helper for Nadeef DB.
 */
public class DBInstaller {

    /**
     * Check whether Nadeef is installed in the targeted database connection.
     * @param conn JDBC connection.
     * @return
     */
    public static boolean isInstalled(Connection conn) throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet resultSet = metaData.getSchemas(null, NadeefConfiguration.getNadeefSchemaName());
        if (resultSet.next()) {
            return true;
        }
        return false;
    }

    /**
     * Install Nadeef in the target database.
     */
    public static void install(Connection conn) throws SQLException {
        Tracer tracer = Tracer.getInstance();
        if (isInstalled(conn)) {
            tracer.info("Nadeef is installed on the database, please try uninstall first.");
            return;
        }

        Statement stat = conn.createStatement();
        stat.execute("CREATE SCHEMA " + NadeefConfiguration.getNadeefSchemaName());
        stat.execute(
                "CREATE TABLE " + NadeefConfiguration.getNadeefSchemaName() + ".violation (" +
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
    public static void uninstall(Connection conn) throws SQLException {
        if (isInstalled(conn)) {
            Statement stat = conn.createStatement();
            stat.execute("DROP SCHEMA " + NadeefConfiguration.getNadeefSchemaName());
            stat.close();
            conn.commit();
        }
    }
}
