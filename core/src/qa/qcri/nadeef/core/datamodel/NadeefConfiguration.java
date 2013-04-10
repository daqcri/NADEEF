/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

/**
 * Nadeef configuration class.
 * TODO: adds XML configuration support.
 */
public class NadeefConfiguration {
    private static boolean testMode = false;
    private String testSchemaName = "nadeef_test";
    private String schemaName = "nadeef";
    private String version = "1.0";
    private String tableName = "violation";

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

    /**
     * Sets the test mode.
     * @param isTestMode
     */
    public static void setTestMode(boolean isTestMode) {
        testMode = isTestMode;
    }

    /**
     * Is Nadeef running in TestMode.
     * @return True when Nadeef is running in test mode.
     */
    public static boolean isTestMode() {
        return testMode;
    }

    /**
     * Gets the Nadeef installed schema name.
     * @return Nadeef DB schema name.
     */
    public String getNadeefSchemaName() {
        if (isTestMode()) {
            return testSchemaName;
        }
        return schemaName;
    }

    /**
     * Gets Nadeef violation table name.
     * @return violation table name.
     */
    public String getNadeefViolationTableName() {
        return tableName;
    }

    /**
     * Gets the Nadeef version.
     * @return Nadeef version.
     */
    public String getNadeefVersion() {
        return version;
    }
    //</editor-fold>
}
