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
import qa.qcri.nadeef.tools.sql.SQLDialectTools;

import java.io.File;
import java.io.Reader;
import java.nio.file.Path;
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
    private static boolean alwaysOverrideTable = true;
    private static HashMap<String, RuleBuilder> ruleExtension = Maps.newHashMap();
    private static Optional<Class> decisionMakerClass;
    private static Path outputPath;
    private static String serverUrl = "localhost";
    private static int serverPort = 9000;

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
        String username = "";
        if (database.containsKey("username")) {
            username = (String)database.get("username");
        }

        String password = "";
        if (database.containsKey("password")) {
            password = (String)database.get("password");
        }

        String type;
        if (database.containsKey("type")) {
            type = (String)database.get("type");
        } else {
            type = "derby";
        }

        dbConfig =
            new DBConfig.Builder()
                .url(url)
                .username(username)
                .password(password)
                .dialect(SQLDialectTools.getSQLDialect(type))
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

        if (general.containsKey("alwaysOverwriteTable")) {
            alwaysOverrideTable = (Boolean)(general.get("alwaysOverwriteTable"));
        }

        if (general.containsKey("fixdecisionmaker")) {
            String className = (String)general.get("fixdecisionmaker");
            Class customizedClass = CommonTools.loadClass(className);
            decisionMakerClass = Optional.of(customizedClass);
        } else {
            decisionMakerClass = Optional.absent();
        }

        if (general.containsKey("outputPath")) {
            String outputPathString = (String)general.get("outputPath");
            File tmpPath = new File(outputPathString);
            if (tmpPath.exists() && tmpPath.isDirectory()) {
                outputPath = tmpPath.toPath();
            } else {
                outputPathString = System.getProperty("user.dir");
                tracer.info(
                    "Cannot find directory " + outputPathString +
                    ", we change to working directory " + outputPathString
                );

                outputPath = new File(outputPathString).toPath();
            }
        }

        JSONObject ruleext = (JSONObject)jsonObject.get("ruleext");
        Set<String> keySet = (Set<String>)ruleext.keySet();
        for (String key : keySet) {
            String builderClassName = (String)ruleext.get(key);
            Class builderClass = CommonTools.loadClass(builderClassName);
            RuleBuilder writer = (RuleBuilder)(builderClass.getConstructor().newInstance());
            ruleExtension.put(key, writer);
        }

        if (jsonObject.containsKey("web")) {
            JSONObject thrift = (JSONObject)jsonObject.get("web");
            serverUrl = (String)thrift.get("url");
            serverPort = ((Long)thrift.get("port")).intValue();
        }
    }

    /**
     * Gets the server url.
     * @return server url.
     */
    public static String getServerUrl() {
        return serverUrl;
    }

    /**
     * Gets server port.
     * @return port number
     */
    public static int getServerPort() {
        return serverPort;
    }

    /**
     * Sets the test mode.
     * @param isTestMode test mode.
     */
    public static void setTestMode(boolean isTestMode) {
        testMode = isTestMode;
    }

    /**
     * Sets AlwaysOverrideTable.
     * @param isAlwaysOverride alwaysOverride mode.
     */
    public static void setAlwaysOverride(boolean isAlwaysOverride) {
        alwaysOverrideTable = isAlwaysOverride;
    }
    /**
     * Is Nadeef running in TestMode.
     * @return True when Nadeef is running in test mode.
     */
    public static boolean isTestMode() {
        return testMode;
    }

    /**
     * Gets the NADEEF output path. Output path is used for writing logs,
     * temporary class files, etc.
     * @return the NADEEF output path.
     */
    public static Path getOutputPath() {
        return outputPath;
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
        String typeName_ = typeName.toLowerCase();
        if (ruleExtension.containsKey(typeName_)) {
            return ruleExtension.get(typeName_);
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
        return "VIOLATION";
    }

    /**
     * Gets Nadeef violation table name.
     * @return violation table name.
     */
    public static String getRepairTableName() {
        return "REPAIR";
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
     * Gets OverwriteTable option.
     * @return OverwriteTable value.
     */
    public static boolean getAlwaysOverrideTable() {
        return alwaysOverrideTable;
    }

    /**
     * Gets the Audit table name.
     * @return audit table name.
     */
    public static String getAuditTableName() {
        return "AUDIT";
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
