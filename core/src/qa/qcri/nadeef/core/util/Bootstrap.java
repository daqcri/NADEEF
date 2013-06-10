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

import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.pipeline.NodeCacheManager;
import qa.qcri.nadeef.tools.DBInstaller;
import qa.qcri.nadeef.tools.Tracer;

import java.io.FileNotFoundException;
import java.io.FileReader;
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
