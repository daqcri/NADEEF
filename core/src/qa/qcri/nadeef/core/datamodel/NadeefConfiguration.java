/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;

import java.io.Reader;
import java.util.HashMap;
import java.util.Set;

/**
 * Nadeef configuration class.
 */
public class NadeefConfiguration {
    private static Tracer tracer = Tracer.getTracer(NadeefConfiguration.class);

    private static boolean testMode = false;
    private static DBConfig dbConfig;
    private static int maxIterationNumber = 1;
    private static boolean alwaysCompile = false;
    private static HashMap<String, RuleBuilder> ruleExtension = Maps.newHashMap();
    private static Optional<Class> decisionMakerClass;
    //<editor-fold desc="Public methods">

    /**
     * Initialize configuration from string.
     * @param reader configuration string.
     */
    @SuppressWarnings("unchecked")
    public synchronized static void initialize(Reader reader) throws Exception {
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

        JSONObject general = (JSONObject)jsonObject.get("general");
        if (general.containsKey("testmode")) {
            testMode = (Boolean)general.get("testmode");
            Tracer.setVerbose(testMode);
        }

        if (general.containsKey("maxIterationNumber")) {
            maxIterationNumber = ((Long)general.get("maxIterationNumber")).intValue();
        }

        if (general.containsKey("alwaysCompile")) {
            alwaysCompile = (Boolean)(general.get("alwaysCompile"));
        }

        if (general.containsKey("fixdecisionmaker")) {
            String className = (String)general.get("fixdecisionmaker");
            Class customizedClass = CommonTools.loadClass(className);
            decisionMakerClass = Optional.of(customizedClass);
        } else {
            decisionMakerClass = Optional.absent();
        }

        JSONObject ruleext = (JSONObject)jsonObject.get("ruleext");
        Set<String> keySet = (Set<String>)ruleext.keySet();
        for (String key : keySet) {
            String builderClassName = (String)ruleext.get(key);
            Class builderClass = CommonTools.loadClass(builderClassName);
            RuleBuilder writer = (RuleBuilder)(builderClass.getConstructor().newInstance());
            ruleExtension.put(key, writer);
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

    /**
     * Gets the <code>DBConfig</code> of Nadeef metadata database.
     * @return meta data <code>DBConfig</code>.
     */
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
        String schemaName = "public";
        return schemaName;
    }

    /**
     * Gets Nadeef violation table name.
     * @return violation table name.
     */
    public static String getViolationTableName() {
        return "violation";
    }

    /**
     * Gets Nadeef violation table name.
     * @return violation table name.
     */
    public static String getRepairTableName() {
        return "repair";
    }

    /**
     * Sets AlwaysCompile value.
     * @param alwaysCompile_ alwaysCompile value.
     */
    public static void setAlwaysCompile(boolean alwaysCompile_) {
        alwaysCompile = alwaysCompile_;
    }

    /**
     * Gets AlwaysCompile option.
     * @return alwaysCompile value.
     */
    public static boolean getAlwaysCompile() {
        return alwaysCompile;
    }

    /**
     * Gets the Nadeef version.
     * @return Nadeef version.
     */
    public static String getVersion() {
        String version = "Alpha";
        return version;
    }

    /**
     * Gets the Audit table name.
     * @return audit table name.
     */
    public static String getAuditTableName() {
        return "audit";
    }

    /**
     * Gets the decision maker class.
     * @return decision maker class. It is absent when user is not providing a customized
     */
    public static Optional<Class> getDecisionMakerClass() {
        return decisionMakerClass;
    }
    //</editor-fold>
}
