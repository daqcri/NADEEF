/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import org.jooq.SQLDialect;

import java.sql.Connection;

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
            return "nadeef_test";
        }
        return "nadeef";
    }

    /**
     * Gets the Nadeef version.
     * @return Nadeef version.
     */
    public String getNadeefVersion() {
        return "1.0";
    }
    //</editor-fold>
}
