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

import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.tools.Tracer;
import qa.qcri.nadeef.tools.sql.SQLDialect;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * NADEEF database installation utility class.
 */
public final class DBInstaller {
    private static Tracer tracer = Tracer.getTracer(DBInstaller.class);

    /**
     * Checks whether NADEEF is installed in the targeted database connection.
     * @param tableName source tableName.
     * @return TRUE when Nadeef is already installed on the database.
     */
    public static boolean isInstalled(String tableName) throws SQLException {
        Connection conn = null;
        try {
            conn = DBConnectionFactory.getNadeefConnection();
            DatabaseMetaData metaData = conn.getMetaData();

            return metaData.getTables(null, null, tableName, null).next() ||
                // TODO: a hack for case sensitive db, should use other methods
                metaData.getTables(null, null, tableName.toLowerCase(), null).next();
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * Install NADEEF on the target database.
     */
    public static void install(
        String violationTableName,
        String repairTableName,
        String auditTableName) throws SQLException {
        Connection conn = null;
        Statement stat = null;

        try {
            if (isInstalled(violationTableName)) {
                tracer.info("Nadeef is installed on the database, please try uninstall first.");
                return;
            }

            SQLDialect dialect = NadeefConfiguration.getDbConfig().getDialect();
            SQLDialectBase dialectManager =
                SQLDialectFactory.getDialectManagerInstance(dialect);

            conn = DBConnectionFactory.getNadeefConnection();
            stat = conn.createStatement();
            // TODO: make tables BNCF
            stat.execute(dialectManager.createViolationTable(violationTableName));
            stat.execute(dialectManager.createRepairTable(repairTableName));
            stat.execute(dialectManager.createAuditTable(auditTableName));

            conn.commit();
        } catch (SQLException ex) {
            tracer.err("SQLException during installing tables.", ex);
            throw ex;
        } finally {
            if (stat != null) {
                stat.close();
            }

            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * Uninstall NADEEF from the target database.
     * @param tableName uninstall table name.
     */
    public static void uninstall(String tableName) throws SQLException {
        Connection conn = null;
        SQLDialectBase dialectManager =
            SQLDialectFactory.getNadeefDialectManagerInstance();
        Statement stat = null;
        if (isInstalled(tableName)) {
            try {
                conn = DBConnectionFactory.getNadeefConnection();
                stat = conn.createStatement();
                stat.execute(dialectManager.dropTable(tableName));
                stat.close();
                conn.commit();
            } catch (Exception ex) {
                tracer.err("Exception during uninstall table " + tableName, ex);
            } finally {
                if (stat != null) {
                    stat.close();
                }

                if (conn != null) {
                    conn.close();
                }
            }
        }
    }
}
