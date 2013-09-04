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

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.Files;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import qa.qcri.nadeef.core.exception.InvalidCleanPlanException;
import qa.qcri.nadeef.core.exception.InvalidRuleException;
import qa.qcri.nadeef.core.util.sql.DBConnectionFactory;
import qa.qcri.nadeef.core.util.sql.DBMetaDataTool;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.core.util.sql.NadeefSQLDialectManagerBase;
import qa.qcri.nadeef.core.util.sql.SQLDialectManagerFactory;
import qa.qcri.nadeef.tools.*;

import java.io.File;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

/**
 * Nadeef cleaning plan.
 */
public class CleanPlan {
    private DBConfig source;
    private Rule rule;
    private static Tracer tracer = Tracer.getTracer(CleanPlan.class);

    // <editor-fold desc="Constructor">
    /**
     * Constructor.
     */
    public CleanPlan(DBConfig sourceConfig, Rule rule) {
        this.source = sourceConfig;
        this.rule = rule;
    }

    // </editor-fold>

    /**
     * Creates a {@link CleanPlan} from JSON string.
     *
     * @param reader JSON string reader.
     * @return <code>CleanPlan</code> object.
     */
    @SuppressWarnings("unchecked")
    public static List<CleanPlan> createCleanPlanFromJSON(Reader reader)
            throws InvalidRuleException, InvalidCleanPlanException {
        Preconditions.checkNotNull(reader);
        JSONObject jsonObject = (JSONObject) JSONValue.parse(reader);

        // a set which prevents generating new tables whenever encounters among
        // multiple rules.
        List<CleanPlan> result = Lists.newArrayList();
        boolean isCSV = false;
        List<Schema> schemas = Lists.newArrayList();

        Connection conn = null;
        NadeefSQLDialectManagerBase dialectManager = null;
        try {
            // ----------------------------------------
            // parsing the source config
            // ----------------------------------------
            JSONObject src = (JSONObject) jsonObject.get("source");

            String type = "derby";
            DBConfig dbConfig = null;
            String username = "";
            String password = "";

            if (src.containsKey("type")) {
                type = (String)src.get("type");
            }

            if (src.containsKey("username")) {
                username = (String)src.get("username");
            }

            if (src.containsKey("password")) {
                password = (String)src.get("password");
            }

            SQLDialect sqlDialect = null;
            switch (type) {
                case "csv":
                    isCSV = true;
                    dbConfig = NadeefConfiguration.getDbConfig();
                    sqlDialect = dbConfig.getDialect();
                    break;
                case "derby":
                default:
                    sqlDialect = SQLDialect.DERBY;
                    break;
                case "postgres":
                    sqlDialect = SQLDialect.POSTGRES;
                    break;
            }

            if (dbConfig == null) {
                dbConfig =
                    new DBConfig.Builder()
                        .username(username)
                        .password(password)
                        .url((String) src.get("url"))
                        .dialect(sqlDialect)
                        .build();
            }

            dialectManager = SQLDialectManagerFactory.getDialectManagerInstance(sqlDialect);

            // ----------------------------------------
            // parsing the rules
            // ----------------------------------------
            // TODO: use token.matches("^\\s*(\\w+\\.?){0,3}\\w\\s*$") to match
            // the table pattern.
            JSONArray ruleArray = (JSONArray) jsonObject.get("rule");
            ArrayList<Rule> rules = Lists.newArrayList();
            List<String> targetTableNames;
            List<String> fileNames = Lists.newArrayList();
            HashSet<String> copiedTables = Sets.newHashSet();

            for (int i = 0; i < ruleArray.size(); i++) {
                schemas.clear();
                fileNames.clear();
                JSONObject ruleObj = (JSONObject) ruleArray.get(i);
                if (isCSV) {
                    // working with CSV
                    List<String> fullFileNames = (List<String>) src.get("file");
                    for (String fullFileName : fullFileNames) {
                        fileNames.add(Files.getNameWithoutExtension(fullFileName));
                    }

                    if (ruleObj.containsKey("table")) {
                        targetTableNames = (List<String>) ruleObj.get("table");
                        Preconditions.checkArgument(
                            targetTableNames.size() <= 2,
                            "NADEEF only supports MAX 2 tables per rule."
                        );

                        // check table whether it already exists.
                        for (String targetTableName : targetTableNames) {
                            boolean isFound = false;
                            for (String fileName : fileNames) {
                                if (fileName.equalsIgnoreCase(targetTableName)) {
                                    isFound = true;
                                    break;
                                }
                            }

                            if (!isFound) {
                                throw new InvalidCleanPlanException("Unknown table name.");
                            }
                        }
                    } else {
                        // if the target table names does not exist, we use
                        // default naming and only the first two tables are touched.
                        targetTableNames = Lists.newArrayList();
                        for (String fileName : fileNames) {
                            targetTableNames.add(fileName);
                            if (targetTableNames.size() == 2) {
                                break;
                            }
                        }
                    }

                    // source is a CSV file, dump it to NADEEF database.
                    conn = DBConnectionFactory.getNadeefConnection();
                    // This hashset is to prevent that tables are dumped for multiple times.
                    for (int j = 0; j < targetTableNames.size(); j++) {
                        File file = CommonTools.getFile(fullFileNames.get(j));
                        // target table name already exists in the hashset.
                        if (!copiedTables.contains(targetTableNames.get(j))) {
                            String tableName =
                                CSVTools.dump(
                                    conn,
                                    dialectManager,
                                    file,
                                    targetTableNames.get(j),
                                    NadeefConfiguration.getAlwaysOverrideTable()
                                );
                            copiedTables.add(targetTableNames.get(j));
                            targetTableNames.set(j, tableName);
                            schemas.add(DBMetaDataTool.getSchema(dbConfig,tableName));
                        }
                    }
                } else {
                    // working with database
                    List<String> sourceTableNames = (List<String>) ruleObj.get("table");
                    for (String tableName : sourceTableNames) {
                        if (!DBMetaDataTool.isTableExist(dbConfig, tableName)) {
                            throw new InvalidCleanPlanException(
                                "The specified table " +
                                tableName +
                                " cannot be found in the source database.");
                        }
                    }

                    if (ruleObj.containsKey("target")) {
                        targetTableNames = (List<String>) ruleObj.get("target");
                    } else {
                        // when user doesn't provide target tables we create a
                        // copy for them
                        // with default table names.
                        targetTableNames = Lists.newArrayList();
                        for (String sourceTableName : sourceTableNames) {
                            targetTableNames.add(sourceTableName + "_copy");
                        }
                    }

                    Preconditions.checkArgument(
                        sourceTableNames.size() == targetTableNames.size() &&
                        sourceTableNames.size() <= 2 &&
                        sourceTableNames.size() >= 1,
                        "Invalid Rule property, rule needs to have one or two tables.");

                    for (int j = 0; j < sourceTableNames.size(); j++) {
                        if (!copiedTables.contains(targetTableNames.get(j))) {
                            DBMetaDataTool.copy(
                                dbConfig,
                                dialectManager,
                                sourceTableNames.get(j),
                                targetTableNames.get(j)
                            );

                            schemas.add(
                                DBMetaDataTool.getSchema(dbConfig, targetTableNames.get(j))
                            );
                            copiedTables.add(targetTableNames.get(j));
                        }
                    }
                }

                type = (String) ruleObj.get("type");
                Rule rule;
                JSONArray value;
                value = (JSONArray) ruleObj.get("value");
                String ruleName = (String) ruleObj.get("name");
                if (Strings.isNullOrEmpty(ruleName)) {
                    // generate default rule name when it is not provided by the user, and
                    // distinguished by the value of the rule.
                    ruleName = "Rule" + CommonTools.toHashCode((String)value.get(0));
                }
                switch (type) {
                case "udf":
                    value = (JSONArray) ruleObj.get("value");
                    Class udfClass =
                        CommonTools.loadClass((String) value.get(0));
                    if (!Rule.class.isAssignableFrom(udfClass)) {
                        throw new InvalidRuleException(
                            "The specified class is not a Rule class."
                        );
                    }

                    rule = (Rule) udfClass.newInstance();
                    // call internal initialization on the rule.
                    rule.initialize(ruleName, targetTableNames);
                    rules.add(rule);
                    break;
                default:
                    RuleBuilder ruleBuilder = NadeefConfiguration.tryGetRuleBuilder(type);
                    if (ruleBuilder != null) {
                        rules.addAll(
                            ruleBuilder.name(ruleName)
                                .schema(schemas)
                                .table(targetTableNames)
                                .value(value)
                                .build()
                        );
                    } else {
                        tracer.err("Unknown Rule type: " + type, null);
                    }
                    break;
                }
            }

            for (int i = 0; i < rules.size(); i++) {
                result.add(new CleanPlan(dbConfig, rules.get(i)));
            }
            return result;
        } catch (Exception ex) {
            if (ex instanceof InvalidRuleException) {
                throw (InvalidRuleException) ex;
            }
            throw new InvalidCleanPlanException(ex);
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {
                    // ignore;
                }
            }
        }
    }

    // <editor-fold desc="Property Getters">

    /**
     * Gets the {@link DBConfig} for the clean source.
     *
     * @return {@link DBConfig}.
     */
    public DBConfig getSourceDBConfig() {
        return source;
    }

    /**
     * Gets the rule.
     *
     * @return rule.
     */
    public Rule getRule() {
        return rule;
    }
    // </editor-fold>
}
