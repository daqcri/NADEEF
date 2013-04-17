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
    private String targetTableName;
    private String sourceUrl;
    private String sourceTableUserName;
    private String sourceTableUserPassword;

    private SQLDialect sqlDialect;
    private List<Rule> rules;

    //<editor-fold desc="Constructor">
    /**
     * Constructor.
     */
    public CleanPlan(
        String targetTableName,
        String sourceUrl,
        String sourceTableUserName,
        String sourceTableUserPassword,
        List<Rule> rules,
        SQLDialect sqlDialect
    ) {
        this.targetTableName = targetTableName;
        this.sourceUrl = sourceUrl;
        this.sourceTableUserName = sourceTableUserName;
        this.sourceTableUserPassword = sourceTableUserPassword;
        this.sqlDialect = sqlDialect;
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

        JSONObject jsonObject = (JSONObject)JSONValue.parse(reader);
        JSONObject src = (JSONObject)jsonObject.get("source");
        SQLDialect sqlDialect;
        String sourceUrl = null;
        String sourceTableUserName = null;
        String sourceTableUserPassword = null;
        String csvTableName = null;
        boolean isCSV = false;

        String type = (String)src.get("type");
        if (type.equalsIgnoreCase("csv")) {
            isCSV = true;
            String file = (String)src.get("file");
            // source is a CSV file, dump it first.
            Connection conn = DBConnectionFactory.createNadeefConnection();
            // TODO; find a way to clean the table after exiting.
            csvTableName = CSVDumper.dump(conn, file);
            sqlDialect = SQLDialect.POSTGRES;
        } else {
            // TODO: support different type of DB.
            sqlDialect = SQLDialect.POSTGRES;

            sourceUrl = (String)src.get("url");
            sourceTableUserName = (String)src.get("username");
            sourceTableUserPassword = (String)src.get("password");
        }

        JSONObject target = (JSONObject)jsonObject.get("target");
        String targetTableName = (String)target.get("table");

        // parse rules.
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
        return
            new CleanPlan(
                targetTableName,
                sourceUrl,
                sourceTableUserName,
                sourceTableUserPassword,
                rules,
                sqlDialect
            );
    }

    //<editor-fold desc="Property Getters">
    public SQLDialect getSqlDialect() {
        return sqlDialect;
    }

    public String getSourceUserPassword() {
        return sourceTableUserPassword;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public String getSourceUrl() {
        return sourceUrl;
    }

    public String getSourceUserName() {
        return sourceTableUserName;
    }

    public List<Rule> getRules() {
        return rules;
    }

    public boolean isSourceDataBase() {
        return sqlDialect != null;
    }
    //</editor-fold>
}
