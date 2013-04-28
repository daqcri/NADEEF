/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.tools.DBInstaller;
import qa.qcri.nadeef.tools.Tracer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;

/**
 * Bootstrapping Nadeef.
 */
public class Bootstrap {
    private static final String configurationFile = "nadeef.conf";

    private Bootstrap() {}

    /**
     * Loads a class in runtime.
     * @param className class name.
     * @return Class type.
     */
    public static Class loadClass(String className) throws ClassNotFoundException {
        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        return classLoader.loadClass(className);
    }

    /**
     * Initialize the Nadeef infrastructure.
     */
    public static synchronized boolean Start() {
        Tracer tracer = Tracer.getTracer(Bootstrap.class);
        try {
            NadeefConfiguration.initialize(new FileReader(configurationFile));
            Connection conn = DBConnectionFactory.createNadeefConnection();
            String violationTableName = NadeefConfiguration.getViolationTableName();
            String repairTableName = NadeefConfiguration.getRepairTableName();
            if (DBInstaller.isInstalled(conn, violationTableName)) {
                DBInstaller.uninstall(conn, violationTableName);
            }
            if (DBInstaller.isInstalled(conn, repairTableName)) {
                DBInstaller.uninstall(conn, repairTableName);
            }

            DBInstaller.install(conn, violationTableName, repairTableName);
            conn.commit();
            conn.close();
        } catch (FileNotFoundException e) {
            tracer.err("Nadeef configuration is not found.");
        } catch (Exception ex) {
            ex.printStackTrace();
            tracer.err("Nadeef database is not able to install, abort.");
            System.exit(1);
        }
        return true;
    }
}
