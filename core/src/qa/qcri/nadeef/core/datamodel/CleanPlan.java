/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.io.Files;
import org.jooq.SQLDialect;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.tools.CSVDumper;
import qa.qcri.nadeef.tools.Tracer;

import java.io.*;
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
            IllegalAccessException {
        Preconditions.checkNotNull(reader);

        SQLDialect sqlDialect;
        String sourceUrl = null;
        String sourceTableUserName = null;
        String sourceTableUserPassword = null;
        String csvTableName = null;
        boolean isCSV = false;

        JSONObject jsonObject = (JSONObject)JSONValue.parse(reader);

        // parsing the source config
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

        // parsing the target config
        // TODO: fill the target parsing

        // parse rules.
        // TODO: adds verification on the rule attributes arguments.
        JSONArray ruleArray = (JSONArray)jsonObject.get("rule");
        ArrayList<Rule> rules = new ArrayList(0);
        for (int i = 0; i < ruleArray.size(); i ++) {
            JSONObject ruleObj = (JSONObject)ruleArray.get(i);
            String name = (String)ruleObj.get("name");
            if (Strings.isNullOrEmpty(name)) {
                name = "rule" + i;
            }

            List<String> tableObjs;
            if (isCSV) {
                tableObjs = Arrays.asList(csvTableName);
            } else {
                tableObjs = (List<String>)ruleObj.get("table");
            }

            if (tableObjs.size() > 2 || tableObjs.size() < 1) {
                throw new IllegalArgumentException(
                    "Invalid Rule property, rule needs to have one or two tables."
                );
            }

            type = (String)ruleObj.get("type");
            Rule rule = null;
            if (type.equalsIgnoreCase("fd")) {
                String value = (String)ruleObj.get("value");
                if (!Strings.isNullOrEmpty(value)) {
                    rule = new FDRule(name, tableObjs, new StringReader(value));
                }
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
}
