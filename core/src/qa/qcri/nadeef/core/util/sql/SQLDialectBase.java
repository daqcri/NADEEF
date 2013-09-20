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

import com.google.common.base.Preconditions;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Column;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * Interface for cross vendor Database methods.
 */
public abstract class SQLDialectBase {

    /**
     * Gets the template file.
     * @return template group file.
     */
    protected abstract STGroupFile getTemplate();

    /**
     * Install violation tables.
     * @param violationTableName violation table name.
     * @return SQL statement.
     */
    public String createViolationTable(String violationTableName) {
        STGroupFile template = Preconditions.checkNotNull(getTemplate());
        ST st = template.getInstanceOf("InstallViolationTable");
        st.add("violationTableName", violationTableName.toUpperCase());
        return st.render();
    }

    /**
     * Install repair tables.
     * @param repairTableName repair table name.
     * @return SQL statement.
     */
    public String createRepairTable(String repairTableName) {
        STGroupFile template = Preconditions.checkNotNull(getTemplate());
        ST st = template.getInstanceOf("InstallRepairTable");
        st.add("repairTableName", repairTableName.toUpperCase());
        return st.render();
    }

    /**
     * Install auditing tables.
     * @param auditTableName audit table name.
     * @return SQL statement.
     */
    public String createAuditTable(String auditTableName) {
        STGroupFile template = Preconditions.checkNotNull(getTemplate());
        ST st = template.getInstanceOf("InstallAuditTable");
        st.add("auditTableName", auditTableName.toUpperCase());
        return st.render();
    }

    /**
     * Next Vid.
     * @param tableName violation table name.
     * @return SQL statement.
     */
    public String nextVid(String tableName) {
        STGroupFile template = Preconditions.checkNotNull(getTemplate());
        ST st = template.getInstanceOf("NextVid");
        st.add("tableName", tableName.toUpperCase());
        return st.render();
    }

    /**
     * Inserts a violation.
     * @param ruleId rule id.
     * @param vid violation id.
     * @param cell violated cell.
     * @return SQL statement.
     */
    // TODO: change to use ST
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

    /**
     * Creates a table in the database from a CSV file header.
     * @param tableName table name.
     * @param content table description.
     * @return SQL statement.
     */
    public String createTableFromCSV(String tableName, String content) {
        STGroupFile template = Preconditions.checkNotNull(getTemplate());
        ST st = template.getInstanceOf("CreateTableFromCSV");
        st.add("tableName", tableName.toUpperCase());
        st.add("content", content);
        return st.render();
    }

    /**
     * Copy table.
     * @param conn connection.
     * @param sourceName source name.
     * @param targetName target name.
     */
    public abstract void copyTable(
        Connection conn,
        String sourceName,
        String targetName
    ) throws SQLException;

    /**
     * Drop table.
     * @param tableName drop table name.
     * @return SQL statement.
     */
    public String dropTable(String tableName) {
        return "DROP TABLE " + tableName;
    }

    /**
     * Drop index.
     * @param indexName index name.
     * @param tableName drop table name.
     * @return SQL statement.
     */
    public String dropIndex(String indexName, String tableName) {
        return "DROP INDEX " + indexName;
    }

    /**
     * Select star..
     * @param tableName table name.
     * @return SQL statement.
     */
    public String selectAll(String tableName) {
        return "SELECT * FROM " + tableName;
    }

    /**
     * Count the number of rows in the table.
     * @param tableName table name.
     * @return SQL statement.
     */
    public abstract String countTable(String tableName);

    /**
     * Limits the select.
     * @param row row number.
     * @return SQL statement.
     */
    public abstract String limitRow(int row);

    /**
     * Inserts values into a table from CSV row used for batch loading.
     * @param metaData column meta data.
     * @param tableName target table name.
     * @param row row value.
     * @return SQL statement.
     */
    public abstract String importFromCSV(
        ResultSetMetaData metaData,
        String tableName,
        String row
    );
}
