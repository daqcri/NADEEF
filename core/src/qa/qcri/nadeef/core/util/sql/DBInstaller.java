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
     * Checks whether the target table is installed in the targeted database connection.
     * @param conn connection.
     * @param tableName source tableName.
     * @return {@code TRUE} when Nadeef is already installed on the database.
     */
    private static boolean isInstalled(
        Connection conn,
        String tableName
    ) throws SQLException {
        try {
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
     * @param connectionPool Connection pool.
     */
    public static void install(DBConnectionPool connectionPool) throws SQLException {
        Connection conn = null;
        Statement stat = null;
        SQLDialect dialect = connectionPool.getNadeefConfig().getDialect();
        SQLDialectBase dialectManager =
            SQLDialectFactory.getDialectManagerInstance(dialect);
        String violationTableName = NadeefConfiguration.getViolationTableName();
        String repairTableName = NadeefConfiguration.getRepairTableName();
        String auditTableName = NadeefConfiguration.getAuditTableName();

        // TODO: make tables BNCF
        try {
            conn = connectionPool.getNadeefConnection();
            stat = conn.createStatement();

            if (isInstalled(conn, violationTableName)) {
                tracer.info("Violation is already installed on the database, skip installing.");
            } else {
                stat.execute(dialectManager.createViolationTable(violationTableName));
            }

            if (isInstalled(conn, repairTableName)) {
                tracer.info("Violation is already installed on the database, skip installing.");
            } else {
                stat.execute(dialectManager.createViolationTable(repairTableName));
            }

            if (isInstalled(conn, auditTableName)) {
                tracer.info("Violation is already installed on the database, skip installing.");
            } else {
                stat.execute(dialectManager.createAuditTable(auditTableName));
            }

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
     * Uninstall NADEEF on the target database.
     * @param connectionPool Connection pool.
     */
    public static void uninstall(DBConnectionPool connectionPool) throws SQLException {
        Connection conn = null;
        Statement stat = null;
        SQLDialect dialect = connectionPool.getNadeefConfig().getDialect();
        SQLDialectBase dialectManager =
            SQLDialectFactory.getDialectManagerInstance(dialect);

        // TODO: make tables BNCF
        try {
            String violationTableName = NadeefConfiguration.getViolationTableName();
            String repairTableName = NadeefConfiguration.getRepairTableName();
            String auditTableName = NadeefConfiguration.getAuditTableName();

            conn = connectionPool.getNadeefConnection();
            stat = conn.createStatement();

            if (isInstalled(conn, violationTableName)) {
                stat.execute(dialectManager.dropTable(violationTableName));
            }

            if (isInstalled(conn, repairTableName)) {
                stat.execute(dialectManager.dropTable(repairTableName));
            }

            if (isInstalled(conn, auditTableName)) {
                stat.execute(dialectManager.dropTable(auditTableName));
            }

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
}
