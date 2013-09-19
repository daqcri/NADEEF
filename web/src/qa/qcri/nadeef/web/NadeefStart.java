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

import qa.qcri.nadeef.tools.CommonTools;

import java.io.IOException;
import java.net.Socket;

/**
 * Nadeef Dashboard launcher.
 */
public final class NadeefStart {
    private static Process derbyProcess;
    private static Process thriftProcess;
    private static final int DERBY_PORT = 1527;
    private static final int THRIFT_PORT = 9091;
    private static final int WEB_PORT = 4567;

    private static final int MAX_TRY_COUNT = 10;
    public static void main(String[] args) {
        try {
            Runtime runtime = Runtime.getRuntime();
            runtime.addShutdownHook(new Thread() {
                public void run() {
                    destroyProcesses();
                }
            });

            if (isPortOccupied(WEB_PORT)) {
                System.err.println("Web port 4567 is occupied, please clear the port first.");
                System.exit(1);
            }

            System.out.print("Start embedded database...");
            derbyProcess =
                runtime.exec("java -d64 -jar out/bin/derbyrun.jar server start");
            if (!waitForService(DERBY_PORT)) {
                System.out.println("FAILED");
                System.exit(1);
            }
            System.out.println("OK");

            System.out.print("Start thrift server...");
            if (CommonTools.isLinux() || CommonTools.isMac()) {
                thriftProcess =
                    Runtime.getRuntime().exec(
                        "java -d64 -cp out/bin/*:. qa.qcri.nadeef.service.NadeefService"
                    );
            } else {
                thriftProcess =
                    Runtime.getRuntime().exec(
                        "java -d64 -cp out/bin/*;. qa.qcri.nadeef.service.NadeefService"
                    );
            }

            if (!waitForService(THRIFT_PORT)) {
                System.out.println("FAILED");
                System.exit(1);
            };
            System.out.println("OK");

            System.out.println("NADEEF Dashboard is live at http://localhost:4567/index.html");
            Dashboard.main(args);
        } catch (Exception ex) {
            destroyProcesses();
            System.err.println("Launching dashboard failed, shutdown.");
            ex.printStackTrace(System.err);
        }
    }

    private static boolean waitForService(int port) {
        int tryCount = 0;
        while (true) {
            if (isPortOccupied(port)) {
                return true;
            }

            if (tryCount == MAX_TRY_COUNT) {
                System.err.println("Waiting for port " + port + " time out.");
                return false;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                // ignore
            }
            tryCount ++;
        }
    }

    private static boolean isPortOccupied(int port) {
        try (Socket ignored = new Socket("localhost", port)) {
            return true;
        } catch (IOException ignored) {
            return false;
        }
    }

    private static void destroyProcesses() {
        if (NadeefStart.derbyProcess != null) {
            derbyProcess.destroy();
        }

        if (thriftProcess != null) {
            thriftProcess.destroy();
        }
    }
}
