/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.jooq.SQLDialect;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.tools.CSVDumper;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
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
    private DBConfig target;
    private List<Rule> rules;

    //<editor-fold desc="Constructor">
    /**
     * Constructor.
     */
    public CleanPlan(
        DBConfig sourceConfig,
        DBConfig targetConfig,
        List<Rule> rules
    ) {
        this.source = sourceConfig;
        this.target = targetConfig;
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
        IOException,
        ClassNotFoundException,
        SQLException,
        InstantiationException,
        IllegalAccessException,
        NoSuchMethodException,
        InvocationTargetException {
        Preconditions.checkNotNull(reader);

        SQLDialect sqlDialect;
        String sourceUrl = null;
        String sourceTableUserName = null;
        String sourceTableUserPassword = null;
        String csvTableName = null;
        boolean isCSV = false;

        JSONObject jsonObject = (JSONObject)JSONValue.parse(reader);

        // ----------------------------------------
        // parsing the source config
        // ----------------------------------------
        JSONObject src = (JSONObject)jsonObject.get("source");
        String type = (String)src.get("type");
        if (type.equalsIgnoreCase("csv")) {
            isCSV = true;
            String fileName = (String)src.get("file");
            // TODO: find a better way to parse the file name.
            fileName = fileName.replace("\\", "\\\\");
            fileName = fileName.replace("\t", "\\t");
            fileName = fileName.replace("\n", "\\n");
            File file = new File(fileName);
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
        DBConfig source =
            new DBConfig(
                sourceTableUserName,
                sourceTableUserPassword,
                sourceUrl,
                sqlDialect
            );

        // ----------------------------------------
        // parsing the target config
        // ----------------------------------------
        // TODO: fill the target parsing

        // ----------------------------------------
        // parsing the rules
        // ----------------------------------------
        // TODO: adds verification on the fd rule attributes arguments.
        // TODO: adds verification on the table name check.
        // TODO: adds verification on the hints.
        // TODO: use token.matches("^\\s*(\\w+\\.?){0,3}\\w\\s*$") to match the pattern.
        JSONArray ruleArray = (JSONArray)jsonObject.get("rule");
        ArrayList<Rule> rules = new ArrayList();
        for (int i = 0; i < ruleArray.size(); i ++) {
            JSONObject ruleObj = (JSONObject)ruleArray.get(i);
            String name = (String)ruleObj.get("name");
            if (Strings.isNullOrEmpty(name)) {
                name = "Rule " + i;
            }

            List<String> tableNames;
            if (isCSV) {
                tableNames = Arrays.asList(csvTableName);
            } else {
                tableNames = (List<String>)ruleObj.get("table");
            }

            if (tableNames.size() > 2 || tableNames.size() < 1) {
                throw new IllegalArgumentException(
                    "Invalid Rule property, rule needs to have one or two tables."
                );
            }

            type = (String)ruleObj.get("type");
            String value = (String)ruleObj.get("value");
            Preconditions.checkArgument(
                !Strings.isNullOrEmpty(value), "Type value cannot be null or empty."
            );

            Rule rule = null;
            switch (type) {
                case "fd":
                    rule = new FDRule(name, tableNames, new StringReader(value));
                    break;
                case "udf":
                    rule = parseUdf(ruleObj, name, tableNames);
                    break;
                default:
                    throw new IllegalArgumentException("Unknown rule type.");
            }

            if (rule != null) {
                rules.add(rule);
            }
        }
        return new CleanPlan(source, null, rules);
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

    private static Rule parseUdf(JSONObject ruleObj, String ruleId, List<String> tableNames)
        throws
            ClassNotFoundException,
            NoSuchMethodException,
            IllegalAccessException,
            InvocationTargetException,
            InstantiationException {
        String className = (String)ruleObj.get("value");
        Preconditions.checkNotNull(className);

        List<Column> verticalColumns = null;
        List<String> horizontals = null;
        Column groupColumn = null;

        JSONObject filterObj = (JSONObject)ruleObj.get("filter");
        if (filterObj != null) {
            JSONArray verticalList = (JSONArray)filterObj.get("vertical");
            if (verticalList != null) {
                verticalColumns = new ArrayList();
                for (int i = 0; i < verticalList.size(); i ++) {
                    String token = (String)verticalList.get(i);
                    Column column;
                    // parsing single or pair table attributes
                    if (!token.contains(".")) {
                        column = new Column(tableNames.get(0), token);
                    } else {
                        column = new Column((String)verticalList.get(i));
                    }
                    verticalColumns.add(column);
                }
            }

            JSONArray horizontalList = (JSONArray)filterObj.get("horizontal");
            if (horizontalList != null) {
                horizontals = new ArrayList();
                for (int i = 0; i < horizontalList.size(); i ++) {
                    horizontals.add((String)horizontalList.get(i));
                }
            }
        }

        JSONObject groupObj = (JSONObject)ruleObj.get("group");
        if (groupObj != null) {
            String groupCellName = (String)groupObj.get("on");
            if (!Strings.isNullOrEmpty(groupCellName)) {
                if (!groupCellName.contains(".")) {
                    groupColumn = new Column(tableNames.get(0), groupCellName);
                } else {
                    groupColumn = new Column(groupCellName);
                }
            }
        }

        ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        Class udfClass = classLoader.loadClass(className);
        if (!Rule.class.isAssignableFrom(udfClass)) {
            throw
                new IllegalArgumentException(
                    "The specified class is not a Rule class."
                );
        }

        Rule rule =
            (Rule)udfClass.getDeclaredConstructor(
                String.class,
                List.class
            ).newInstance(ruleId, tableNames);

        if (groupColumn != null) {
            rule.setGroupColumn(groupColumn);
        }

        if (verticalColumns != null) {
            rule.setVerticalFilter(verticalColumns);
        }

        if (horizontals != null) {
            rule.setHorizontalFilter(horizontals);
        }
        return rule;
    }
}
