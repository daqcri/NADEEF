/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import qa.qcri.nadeef.core.util.RuleWriter;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.Tracer;

import java.io.Reader;
import java.util.HashMap;
import java.util.Set;

/**
 * Nadeef configuration class.
 * TODO: adds XML configuration support.
 */
public class NadeefConfiguration {
    private static Tracer tracer = Tracer.getTracer(NadeefConfiguration.class);

    private static boolean testMode = false;
    private static String url;
    private static String userName;
    private static String password;
    private static String type;
    private static String violationTable;
    private static String repairTable;
    private static String auditTable;
    @SuppressWarnings("FieldCanBeLocal")
    private static String schemaName = "public";
    @SuppressWarnings("FieldCanBeLocal")
    private static String version = "1.0";
    private static int maxIterationNumber = 1;
    private static HashMap<String, RuleWriter> ruleExtension = Maps.newHashMap();
    //<editor-fold desc="Public methods">

    /**
     * Initialize configuration from string.
     * @param reader configuration string.
     */
    public synchronized static void initialize(Reader reader) {
        Preconditions.checkNotNull(reader);
        JSONObject jsonObject = (JSONObject)JSONValue.parse(reader);
        JSONObject database = (JSONObject)jsonObject.get("database");
        url = (String)database.get("url");
        userName = (String)database.get("username");
        password = (String)database.get("password");
        type = (String)database.get("type");

        JSONObject violation = (JSONObject)jsonObject.get("violation");
        violationTable = (String)violation.get("name");

        JSONObject repair = (JSONObject)jsonObject.get("repair");
        repairTable = (String)repair.get("name");

        JSONObject audit = (JSONObject)jsonObject.get("audit");
        auditTable = (String)audit.get("name");

        JSONObject general = (JSONObject)jsonObject.get("general");
        testMode = (Boolean)general.get("testmode");
        if (general.containsKey("maxIterationNumber")) {
            maxIterationNumber = (Integer)general.get("maxIterationNumber");
        }

        JSONObject ruleext = (JSONObject)jsonObject.get("ruleext");
        Set<String> keySet = ruleext.keySet();
        for (String key : keySet) {
            String builderClassName = (String)ruleext.get(key);
            try {
                Class builderClass = CommonTools.loadClass(builderClassName);
                RuleWriter writer = (RuleWriter)builderClass.newInstance();
                ruleExtension.put(key, writer);
            } catch (Exception e) {
                tracer.err("Loading Rule extension " + key + " failed: " + e.getMessage());
                e.printStackTrace();
            }
        }
        testMode = (Boolean)general.get("fd");
        if (general.containsKey("maxIterationNumber")) {
            maxIterationNumber = (Integer)general.get("maxIterationNumber");
        }
    }

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

    public static String getUrl() {
        return url;
    }

    public static String getUserName() {
        return userName;
    }

    public static String getPassword() {
        return password;
    }

    public static String getType() {
        return type;
    }

    /**
     * Try gets the rule builder from the extensions.
     * @param typeName type name.
     * @return RuleBuilder instance.
     */
    public static RuleWriter tryGetRuleBuilder(String typeName) {
        if (ruleExtension.containsKey(typeName)) {
            return ruleExtension.get(typeName);
        }
        return null;
    }

    /**
     * Gets the Nadeef installed schema name.
     * @return Nadeef DB schema name.
     */
    public static int getMaxIterationNumber() {
        return maxIterationNumber;
    }

    /**
     * Gets the Nadeef installed schema name.
     * @return Nadeef DB schema name.
     */
    public static String getSchemaName() {
        return schemaName;
    }

    /**
     * Gets Nadeef violation table name.
     * @return violation table name.
     */
    public static String getViolationTableName() {
        return violationTable;
    }

    /**
     * Gets Nadeef violation table name.
     * @return violation table name.
     */
    public static String getRepairTableName() {
        return repairTable;
    }

    /**
     * Gets the Nadeef version.
     * @return Nadeef version.
     */
    public static String getVersion() {
        return version;
    }

    public static String getAuditTableName() {
        return auditTable;
    }
    //</editor-fold>
}
