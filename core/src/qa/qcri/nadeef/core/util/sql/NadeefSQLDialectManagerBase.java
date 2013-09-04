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

package qa.qcri.nadeef.core.util.sql;

import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.tools.SQLDialectManagerBase;

/**
 * Interface for cross vendor Database methods.
 */
public abstract class NadeefSQLDialectManagerBase extends SQLDialectManagerBase {
    /**
     * Install violation tables.
     * @param violationTableName violation table name.
     * @return SQL statement.
     */
    public abstract String createViolationTable(String violationTableName);

    /**
     * Install repair tables.
     * @param repairTableName repair table name.
     * @return SQL statement.
     */
    public abstract String createRepairTable(String repairTableName);

    /**
     * Install auditing tables.
     * @param auditTableName audit table name.
     * @return SQL statement.
     */
    public abstract String createAuditTable(String auditTableName);

    /**
     * Next Vid.
     * @param tableName violation table name.
     * @return SQL statement.
     */
    public abstract String nextVid(String tableName);

    /**
     * Inserts a violation.
     * @param ruleId rule id.
     * @param vid violation id.
     * @param cell violated cell.
     * @return SQL statement.
     */
    public String insertViolation(String ruleId, int vid, Cell cell) {
        StringBuilder sqlBuilder = new StringBuilder(1024);
        Column column = cell.getColumn();
        sqlBuilder
            .append("INSERT INTO VIOLATION VALUES (")
            .append(vid)
            .append(",\'")
            .append(ruleId)
            .append("\',\'")
            .append(column.getTableName())
            .append("\',")
            .append(cell.getTupleId())
            .append(",\'")
            .append(column.getColumnName())
            .append("\', \'");

        Object value = cell.getValue();
        if (value == null) {
            sqlBuilder.append("null");
        } else {
            sqlBuilder.append(value.toString());
        }
        sqlBuilder.append("\')");
        return sqlBuilder.toString();
    }
}
