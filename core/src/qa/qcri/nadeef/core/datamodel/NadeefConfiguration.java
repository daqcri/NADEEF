/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.DBConfig;
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
    private static DBConfig dbConfig;
    private static String violationTable;
    private static String repairTable;
    private static String auditTable;
    private static String schemaName = "public";
    private static String version = "1.0";
    private static int maxIterationNumber = 1;
    private static HashMap<String, RuleBuilder> ruleExtension = Maps.newHashMap();

    //<editor-fold desc="Public methods">

    /**
     * Initialize configuration from string.
     * @param reader configuration string.
     */
    public synchronized static void initialize(Reader reader) {
        Preconditions.checkNotNull(reader);
        JSONObject jsonObject = (JSONObject)JSONValue.parse(reader);
        JSONObject database = (JSONObject)jsonObject.get("database");
        String url = (String)database.get("url");
        String userName = (String)database.get("username");
        String password = (String)database.get("password");
        String type = (String)database.get("type");

        DBConfig.Builder builder = new DBConfig.Builder();
        dbConfig =
            builder
                .url(url)
                .username(userName)
                .password(password)
                .dialect(CommonTools.getSQLDialect(type))
                .build();

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
                RuleBuilder writer = (RuleBuilder)builderClass.getConstructor().newInstance();
                ruleExtension.put(key, writer);
            } catch (Exception e) {
                tracer.err("Loading Rule extension " + key + " failed: ", e);
                e.printStackTrace();
            }
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

    public static DBConfig getDbConfig() {
        return dbConfig;
    }

    /**
     * Try gets the rule builder from the extensions.
     * @param typeName type name.
     * @return RuleBuilder instance.
     */
    public static RuleBuilder tryGetRuleBuilder(String typeName) {
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
