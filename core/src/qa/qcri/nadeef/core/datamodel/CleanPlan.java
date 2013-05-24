/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import qa.qcri.nadeef.core.exception.InvalidCleanPlanException;
import qa.qcri.nadeef.core.exception.InvalidRuleException;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.DBMetaDataTool;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.tools.*;

import java.io.File;
import java.io.Reader;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Nadeef cleaning plan.
 */
public class CleanPlan {
    private DBConfig source;
    private List<Rule> rules;
    private static List<String> tableNames;
    private static List<Schema> schemas;
    private static Tracer tracer = Tracer.getTracer(CleanPlan.class);

    //<editor-fold desc="Constructor">
    /**
     * Constructor.
     */
    public CleanPlan(
        DBConfig sourceConfig,
        List<Rule> rules
    ) {
        this.source = sourceConfig;
        this.rules = rules;
    }

    //</editor-fold>

    /**
     * Creates a <code>CleanPlan</code> from JSON string.
     * @param reader JSON string reader.
     * @return <code>CleanPlan</code> object.
     */
    public static CleanPlan createCleanPlanFromJSON(Reader reader)
        throws
            InvalidRuleException,
            InvalidCleanPlanException {
        Preconditions.checkNotNull(reader);
        // a set which prevents generating new tables whenever encounters among multiple rules.
        SQLDialect sqlDialect;
        String csvTableName = null;

        boolean isCSV = false;
        tableNames = Lists.newArrayList();
        schemas = Lists.newArrayList();
        JSONObject jsonObject = (JSONObject)JSONValue.parse(reader);

        Connection conn = null;
        try {
            // ----------------------------------------
            // parsing the source config
            // ----------------------------------------
            JSONObject src = (JSONObject)jsonObject.get("source");
            String type = (String)src.get("type");
            DBConfig dbConfig;

            switch (type) {
                case "csv":
                    isCSV = true;
                    String fileName = (String)src.get("file");
                    File file = FileHelper.getFile(fileName);
                    // source is a CSV file, dump it to NADEEF database first.
                    conn = DBConnectionFactory.getNadeefConnection();
                    csvTableName = CSVDumper.dump(conn, file);
                    dbConfig = NadeefConfiguration.getDbConfig();
                    break;
                default:
                    // TODO: support different type of DB.
                    sqlDialect = SQLDialect.POSTGRES;
                    DBConfig.Builder builder = new DBConfig.Builder();
                    dbConfig =
                        builder.username((String)src.get("username"))
                            .password((String)src.get("password"))
                            .url((String)src.get("url"))
                            .dialect(sqlDialect)
                            .build();
            }

            // Initialize the connection pool
            DBConnectionFactory.initializeSource(dbConfig);

            // ----------------------------------------
            // parsing the rules
            // ----------------------------------------
            // TODO: use token.matches("^\\s*(\\w+\\.?){0,3}\\w\\s*$") to match the pattern.
            JSONArray ruleArray = (JSONArray)jsonObject.get("rule");
            ArrayList<Rule> rules = Lists.newArrayList();
            for (int i = 0; i < ruleArray.size(); i ++) {
                JSONObject ruleObj = (JSONObject)ruleArray.get(i);
                List<String> sourceTableNames;
                List<String> targetTableNames;
                if (isCSV) {
                    targetTableNames = Arrays.asList(csvTableName);
                    tableNames.add(csvTableName);
                    schemas.add(DBMetaDataTool.getSchema(dbConfig, csvTableName));
                } else {
                    sourceTableNames = (List<String>)ruleObj.get("table");
                    for (String tableName : sourceTableNames) {
                        if (!DBMetaDataTool.isTableExist(tableName)) {
                            throw
                                new InvalidCleanPlanException (
                                    "The specified table " + tableName +
                                        " cannot be found in the source database."
                                );
                        }
                    }
                    tableNames.addAll(sourceTableNames);
                    targetTableNames = (List<String>)ruleObj.get("target");
                    if (targetTableNames == null) {
                        // when user doesn't provide target tables we create a copy for them
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
                        "Invalid Rule property, rule needs to have one or two tables."
                    );

                    for (int j = 0; j < sourceTableNames.size(); j ++) {
                        DBMetaDataTool.copy(
                            sourceTableNames.get(j),
                            targetTableNames.get(j)
                        );
                        schemas.add(
                            DBMetaDataTool.getSchema(dbConfig, targetTableNames.get(j))
                        );
                    }
                }

                type = (String)ruleObj.get("type");
                Rule rule;
                JSONArray value;
                value = (JSONArray)ruleObj.get("value");
                String ruleName = (String)ruleObj.get("name");
                if (Strings.isNullOrEmpty(ruleName)) {
                    // generate default rule name when it is not provided by the user, and
                    // distinguished by the value of the rule.
                    ruleName = "Rule" + CommonTools.toHashCode((String)value.get(0));
                }
                switch (type) {
                    case "udf":
                        value = (JSONArray)ruleObj.get("value");
                        Class udfClass = CommonTools.loadClass((String)value.get(0));
                        if (!Rule.class.isAssignableFrom(udfClass)) {
                            throw
                                new InvalidRuleException(
                                    "The specified class is not a Rule class."
                                );
                        }

                        rule = (Rule)udfClass.newInstance();
                        // call internal initialization on the rule.
                        rule.initialize(ruleName, targetTableNames);
                        rules.add(rule);
                        break;
                    default:
                        RuleBuilder ruleBuilder = NadeefConfiguration.tryGetRuleBuilder(type);
                        if (ruleBuilder != null) {
                            rules.addAll(
                                ruleBuilder
                                    .name(ruleName)
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
            return new CleanPlan(dbConfig, rules);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new InvalidCleanPlanException(ex.getMessage());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException ex) {}
            }
        }
    }

    //<editor-fold desc="Property Getters">

    /**
     * Gets the <code>DBConfig</code> for the clean source.
     * @return <code>DBConfig</code>.
     */
    public DBConfig getSourceDBConfig() {
        return source;
    }

    /**
     * Gets the rules in the <code>CleanPlan</code>.
     * @return a list of <code>Rule</code>.
     */
    public List<Rule> getRules() {
        return rules;
    }

    /**
     * Gets the list of table names in the CleanPlan.
     * @return a list of table names in the CleanPlan.
     */
    public List<String> getTableNames() {
        return tableNames;
    }

    //</editor-fold>
}
