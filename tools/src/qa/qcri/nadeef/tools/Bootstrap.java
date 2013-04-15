/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.pipeline.NodeCacheManager;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.Tracer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.SQLException;

/**
 * Bootstrapping Nadeef.
 */
public class Bootstrap {
    private static boolean isStarted;
    private static final String configurationFile = ".\\nadeef.conf";

    /**
     * Initialize the Nadeef infrastructure.
     */
    public static synchronized void Start() {
        Tracer tracer = Tracer.getTracer(Bootstrap.class);
        try {
            NadeefConfiguration.initialize(new FileReader(configurationFile));
            DBInstaller.install(
                DBConnectionFactory.createNadeefConnection(), new NadeefConfiguration()
            );
        } catch (FileNotFoundException e) {
            tracer.err("Nadeef configuration is not found.");
        } catch (Exception ex) {
            ex.printStackTrace();
            tracer.err("Nadeef database is not able to install, abort.");
            System.exit(1);
        }
        isStarted = true;
    }
}
