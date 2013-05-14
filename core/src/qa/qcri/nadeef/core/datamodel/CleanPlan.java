/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.jooq.SQLDialect;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 * Nadeef cleaning plan.
 */
public class CleanPlan {
    private DBConfig source;
    private List<Rule> rules;
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
        Set<String> generatedTables = Sets.newHashSet();
        SQLDialect sqlDialect;
        String sourceUrl;
        String sourceTableUserName;
        String sourceTableUserPassword;
        String csvTableName = null;
        boolean isCSV = false;

        JSONObject jsonObject = (JSONObject)JSONValue.parse(reader);

        try {
            // ----------------------------------------
            // parsing the source config
            // ----------------------------------------
            JSONObject src = (JSONObject)jsonObject.get("source");
            String type = (String)src.get("type");
            if (type.equalsIgnoreCase("csv")) {
                isCSV = true;
                String fileName = (String)src.get("file");
                File file = FileHelper.getFile(fileName);
                // source is a CSV file, dump it first.
                Connection conn = DBConnectionFactory.createNadeefConnection();
                // TODO: find a way to clean the table after exiting.
                csvTableName = CSVDumper.dump(conn, file);
                sqlDialect = SQLDialect.POSTGRES;
                conn.close();
                sourceUrl = NadeefConfiguration.getUrl();
                sourceTableUserName = NadeefConfiguration.getUserName();
                sourceTableUserPassword = NadeefConfiguration.getPassword();
            } else {
                // TODO: support different type of DB.
                sqlDialect = SQLDialect.POSTGRES;
                sourceUrl = (String)src.get("url");
                sourceTableUserName = (String)src.get("username");
                sourceTableUserPassword = (String)src.get("password");
            }
            DBConfig.Builder builder = new DBConfig.Builder();
            DBConfig dbConfig =
                builder.username(sourceTableUserName)
                       .password(sourceTableUserPassword)
                       .url(sourceUrl)
                       .dialect(sqlDialect)
                       .build();

            // ----------------------------------------
            // parsing the rules
            // ----------------------------------------
            // TODO: adds verification on the fd rule attributes arguments.
            // TODO: adds verification on the table name check.
            // TODO: use token.matches("^\\s*(\\w+\\.?){0,3}\\w\\s*$") to match the pattern.
            JSONArray ruleArray = (JSONArray)jsonObject.get("rule");
            ArrayList<Rule> rules = Lists.newArrayList();
            for (int i = 0; i < ruleArray.size(); i ++) {
                JSONObject ruleObj = (JSONObject)ruleArray.get(i);
                List<String> sourceTableNames;
                List<String> targetTableNames;
                if (isCSV) {
                    targetTableNames = Arrays.asList(csvTableName);
                } else {
                    sourceTableNames = (List<String>)ruleObj.get("table");
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
                            dbConfig,
                            sourceTableNames.get(j),
                            targetTableNames.get(j)
                        );
                    }
                }

                type = (String)ruleObj.get("type");
                Rule rule = null;
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
                                new IllegalArgumentException(
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
                                    .table(targetTableNames)
                                    .value(value)
                                    .build()
                            );
                        } else {
                            tracer.err("Unknown Rule type: " + type);
                        }

                        break;
                }

            }
            return new CleanPlan(dbConfig, rules);
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new InvalidCleanPlanException(ex.getMessage());
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

    //</editor-fold>
}
