/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means “Clean” in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.util;

import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.pipeline.NodeCacheManager;
import qa.qcri.nadeef.tools.DBInstaller;
import qa.qcri.nadeef.tools.Tracer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * Bootstrapping prepares runtime environment for NADEEF. It is invoked before NADEEF starts
 * and also after NADEEF exits.
 */
public class Bootstrap {
    private static boolean isStarted;
    private static final String configurationFile = "nadeef.conf";
    private static Tracer tracer = Tracer.getTracer(Bootstrap.class);

    private Bootstrap() {}

    /**
     * Shutdown NADEEF.
     */
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
     * Bootstrap NADEEF. It tries to load the NADEEF configuration file and install
     * metadata tables in the NADEEF database.
     */
    public static synchronized boolean start() {
        if (isStarted) {
            tracer.info("Nadeef is already started.");
            return true;
        }

        Connection conn = null;
        try {
            NadeefConfiguration.initialize(new FileReader(configurationFile));
            // set the logging directory
            Path outputPath = NadeefConfiguration.getOutputPath();
            Tracer.setLoggingDir(outputPath.toString());

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
            tracer.err(
                "There is something wrong with the NADEEF config. Nadeef database is not " +
                "able to install, abort.",
                ex
            );
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
