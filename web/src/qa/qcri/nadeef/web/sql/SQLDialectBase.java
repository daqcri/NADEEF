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

package qa.qcri.nadeef.web.sql;

import org.stringtemplate.v4.STGroupFile;

/**
 * Abstract class for cross-vendor DB compatibility.
 */
public abstract class SQLDialectBase {

    /**
     * Gets the template file.
     * @return template file.
     */
    public abstract STGroupFile getTemplate();

    /**
     * Install Rule table.
     */
    public abstract String installRule();

    /**
     * Install RuleType table.
     */
    public abstract String installRuleType();

    /**
     * Query Violation table.
     * @return query SQL.
     */
    public String queryViolation() {
        return "select vid as Vid, rid as RuleName, tablename as TableName, " +
            "tupleid as TupleID, attribute as ColumnName, value as Value from VIOLATION";
    }

    /**
     * Delete violation.
     * @return query SQL.
     */
    public String deleteViolation() {
        return "DELETE FROM VIOLATION";
    }

    /**
     * Query Table.
     * @param tableName query table name.
     * @return query SQL.
     */
    public String queryTable(String tableName) {
        return "SELECT * FROM " + tableName;
    }

    /**
     * Query supported rules.
     * @return SQL query.
     */
    public String queryRule() {
        return "select x.name, y.name, x.code, x.table1, x.table2, x.java_code from RULE x " +
            "inner join RULETYPE y on x.type = y.type order by x.name";

    }

    /**
     * Query supported rules.
     * @return SQL query.
     */
    public String queryRule(String ruleName) {
        return "select x.name, y.name, x.code, x.table1, x.table2, x.java_code from RULE x " +
            "inner join RULETYPE y on x.type = y.type where x.name = '" + ruleName + "'";

    }

    /**
     * Delete a rule.
     * @param name rule name.
     * @return SQL query.
     */
    public String deleteRule(String name) {
        return "DELETE FROM RULE WHERE name = '" + name + "'";
    }

    /**
     * Insert a new rule.
     * @param type type.
     * @param code code.
     * @param table1 table 1.
     * @param table2 table 2.
     * @param name rule name.
     * @return SQL query.
     */
    public abstract String insertRule(
        String type, String code, String table1, String table2, String name
    );

    /**
     * Query attribute distribution.
     * @return SQL query.
     */
    public String queryAttribute() {
        return "SELECT ATTRIBUTE, COUNT(*) FROM VIOLATION GROUP BY ATTRIBUTE";
    }

    /**
     * Query rule distribution.
     * @return SQL query.
     */
    public String queryRuleDistribution() {
        return "select rid, count(distinct(tupleid)) as tuple, " +
            "count(distinct(tablename)) as tablecount from VIOLATION group by rid";
    }

    /**
     * Query distinct table from violation.
     * @return SQL query.
     */
    public String queryDistinctTable() {
        return "select distinct(tablename) from violation";
    }

    /**
     * Query table count.
     * @param tableName table name.
     * @return SQL query.
     */
    public String countTable(String tableName) {
        return "select count(*) from " + tableName;
    }

    /**
     * Count distinct violation.
     * @return SQL query.
     */
    public String countViolation() {
        return "select count(distinct(tupleid)) from violation";
    }

    /**
     * Query violation relations.
     * @return SQL query.
     */
    public String queryViolationRelation() {
        return "SELECT DISTINCT(VID), TUPLEID FROM VIOLATION ORDER BY VID";
    }

    /**
     * Query top K violated rule.
     * @param k K.
     * @return SQL.
     */
    public abstract String queryTopK(int k);
}
