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

package qa.qcri.nadeef.web;

import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.tools.Tracer;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.nio.file.Path;

/**
 * Bootstrap class which initialize the Database and Configuration.
 *
 *
 */
public final class Bootstrap {
    /**
     * Default configuration file.
     */
    private static String fileName = "nadeef.conf";

    /**
     * Nadeef thrift client.
     */
    private static NadeefClient nadeefClient;

    /**
     * Gets NADEEF Thrift client.
     * @return nadeef client.
     */
    public static NadeefClient getNadeefClient() {
        return nadeefClient;
    }

    /**
     * Initialize Dashboard.
     */
    public static void start() {
        start(fileName);
    }

    public static void start(String fileName) {
        try {
            NadeefConfiguration.initialize(new FileReader(fileName));
            // set the logging directory
            Path outputPath = NadeefConfiguration.getOutputPath();
            Tracer.setLoggingPrefix("dashboard");
            Tracer.setLoggingDir(outputPath.toString());
            Tracer tracer = Tracer.getTracer(Bootstrap.class);
            tracer.verbose("Tracer initialized at " + outputPath.toString());

            // install the meta data
            DBInstaller.installMetaData(NadeefConfiguration.getDbConfig());

            // initialize nadeef client
            NadeefClient.initialize(
                NadeefConfiguration.getServerUrl(),
                NadeefConfiguration.getServerPort()
            );

            nadeefClient = NadeefClient.getInstance();
        } catch (FileNotFoundException ex) {
            System.err.println("Nadeef Configuration cannot be found.");
            ex.printStackTrace();
            System.exit(1);
        } catch (Exception ex) {
            System.err.println("NADEEF initialization failed, please check the log for detail.");
            ex.printStackTrace();
            System.exit(1);
        }
    }
}
