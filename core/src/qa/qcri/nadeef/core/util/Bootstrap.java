/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.pipeline.NodeCacheManager;
import qa.qcri.nadeef.tools.DBInstaller;
import qa.qcri.nadeef.tools.Tracer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Bootstrapping Nadeef.
 */
public class Bootstrap {
    private static boolean isStarted;
    private static final String configurationFile = "nadeef.conf";
    private static Tracer tracer = Tracer.getTracer(Bootstrap.class);

    private Bootstrap() {}

    public static synchronized void shutdown() {
        if (isStarted) {
            DBConnectionFactory.shutdown();
            NodeCacheManager cacheManager = NodeCacheManager.getInstance();
            cacheManager.clear();
            // try to collect the resources (removing views)
            System.gc();
            isStarted = false;
        }
    }

    /**
     * Initialize the Nadeef infrastructure.
     */
    public static synchronized boolean start() {
        if (isStarted) {
            tracer.info("Nadeef is already started.");
            return true;
        }

        Connection conn = null;
        try {
            NadeefConfiguration.initialize(new FileReader(configurationFile));
            DBConnectionFactory.initializeNadeefConnectionPool();

            conn = DBConnectionFactory.getNadeefConnection();
            String violationTableName = NadeefConfiguration.getViolationTableName();
            String repairTableName = NadeefConfiguration.getRepairTableName();
            String auditTableName = NadeefConfiguration.getAuditTableName();

            if (DBInstaller.isInstalled(conn, violationTableName)) {
                DBInstaller.uninstall(conn, violationTableName);
            }

            if (DBInstaller.isInstalled(conn, repairTableName)) {
                DBInstaller.uninstall(conn, repairTableName);
            }

            if (DBInstaller.isInstalled(conn, auditTableName)) {
                DBInstaller.uninstall(conn, auditTableName);
            }

            DBInstaller.install(conn, violationTableName, repairTableName, auditTableName);
            conn.commit();
        } catch (FileNotFoundException ex) {
            tracer.err("Nadeef configuration is not found.", ex);
        } catch (Exception ex) {
            ex.printStackTrace();
            tracer.err("Nadeef database is not able to install, abort.", ex);
            System.exit(1);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
        isStarted = true;
        return true;
    }
}
