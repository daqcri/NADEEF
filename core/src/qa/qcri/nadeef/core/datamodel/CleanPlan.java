/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import org.jooq.SQLDialect;

/**
 * Nadeef cleaning plan.
 */
public class CleanPlan {
    private String sourceTableName;
    private String targetTableName;
    private String sourceTableUrl;
    private String sourceTableUserName;
    private String sourceTableUserPassword;
    private SQLDialect sqlDialect;
    private Rule[] rules;

    /**
     * Constructor.
     */
    public CleanPlan(
        String sourceTableName,
        String targetTableName,
        String sourceTableUrl,
        String sourceTableUserName,
        String sourceTableUserPassword,
        SQLDialect sqlDialect
    ) {
        this.sourceTableName = sourceTableName;
        this.targetTableName = targetTableName;
        this.sourceTableUrl = sourceTableUrl;
        this.sourceTableUserName = sourceTableUserName;
        this.sourceTableUserPassword = sourceTableUserPassword;
        this.sqlDialect = sqlDialect;
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

    public String getSourceTableUrl() {
        return sourceTableUrl;
    }

    public String getSourceTableUserName() {
        return sourceTableUserName;
    }

    public Rule[] getRules() {
        return rules;
    }
    //</editor-fold>
}
