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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Nadeef cleaning plan.
 */
public class CleanPlan {
    private String sourceTableName;
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
        String sourceTableName,
        String targetTableName,
        String sourceUrl,
        String sourceTableUserName,
        String sourceTableUserPassword,
        List<Rule> rules,
        SQLDialect sqlDialect
    ) {
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.sourceUrl = sourceUrl;
        this.sourceTableUserName = sourceTableUserName;
        this.sourceTableUserPassword = sourceTableUserPassword;
        this.sqlDialect = sqlDialect;
        this.rules = rules;
    }

    /**
     * Constructor from a JSON string.
     * @param reader
     * @throws IOException
     */
    public CleanPlan(StringReader reader) {
    }
    //</editor-fold>

    /**
     * Creates a <code>CleanPlan</code> from JSON string.
     * @param reader JSON string reader.
     * @return <code>CleanPlan</code> object.
     */
    public static CleanPlan createCleanPlanFromJSON(Reader reader) {
        Preconditions.checkNotNull(reader);

        JSONObject jsonObject = (JSONObject)JSONValue.parse(reader);
        JSONObject src = (JSONObject)jsonObject.get("source");
        String sourceUrl = (String)src.get("url");
        String sourceTableUserName = (String)src.get("username");
        String sourceTableUserPassword = (String)src.get("password");
        String sourceTableName = (String)src.get("table");

        JSONObject target = (JSONObject)jsonObject.get("target");
        String targetTableName = (String)target.get("table");

        // TODO: make it support for multiple dialects.
        String dialect = (String)src.get("dialect");
        SQLDialect sqlDialect = SQLDialect.POSTGRES;

        // parse rules.
        JSONArray ruleArray = (JSONArray)jsonObject.get("rule");
        ArrayList<Rule> rules = new ArrayList(0);
        for (int i = 0; i < rules.size(); i ++) {
            JSONObject ruleObj = (JSONObject)ruleArray.get(i);
            String name = (String)ruleObj.get("name");
            if (Strings.isNullOrEmpty(name)) {
                name = "rule" + i;
            }
            String type = (String)ruleObj.get("type");
            Rule rule = null;
            if (type.equalsIgnoreCase("fd")) {
                String value = (String)ruleObj.get("value");
                if (Strings.isNullOrEmpty(value)) {
                    rule = new FDRule(name, new StringReader(value));
                }
            }
            if (rule != null) {
                rules.add(rule);
            }
        }
        return
            new CleanPlan(
                sourceTableName,
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

    public String getSourceTableUserPassword() {
        return sourceTableUserPassword;
    }

    public String getSourceTableName() {
        return sourceTableName;
    }

    public String getTargetTableName() {
        return targetTableName;
    }

    public String getSourceUrl() {
        return sourceUrl;
    }

    public String getSourceTableUserName() {
        return sourceTableUserName;
    }

    public List<Rule> getRules() {
        return rules;
    }
    //</editor-fold>
}
