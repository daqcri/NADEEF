/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

/**
 * Nadeef configuration class.
 * TODO: adds XML configuration support.
 */
public class NadeefConfiguration {
    private static boolean testMode;

    //<editor-fold desc="Singleton">
    private static NadeefConfiguration instance;
    private NadeefConfiguration() {}

    public synchronized static NadeefConfiguration getInstance() {
        if (instance == null) {
            instance = new NadeefConfiguration();
        }
        return instance;
    }
    //</editor-fold>

    //<editor-fold desc="Public methods">
    public static void setTestMode(boolean isTestMode) {
        testMode = isTestMode;
    }

    public static boolean isTestMode() {
        return testMode;
    }

    public String getNadeefSchemaName() {
        if (isTestMode()) {
            return "nadeef_test";
        }
        return "nadeef";
    }

    public String getNadeefVersion() {
        return "1.0";
    }
    //</editor-fold>
}
