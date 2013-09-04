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

import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupFile;

import java.io.File;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

/**
 * Database manager for Apache Derby database.
 */
public class DerbySQLManager extends NadeefSQLDialectManagerBase {
    public static STGroupFile groupFile =
        new STGroupFile(
            "qa*qcri*nadeef*core*util*sql*template*DerbyTemplate.stg".replace(
                "*", File.separator
            ), '$', '$');

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyTable(Statement stat, String sourceName, String targetName)
            throws SQLException {
        stat.execute(
            "CREATE TABLE " + targetName + " AS SELECT * FROM " + sourceName + " WITH NO DATA"
        );
        stat.execute("INSERT INTO " + targetName + " SELECT * FROM " + sourceName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String countTable(String tableName) {
        ST st = groupFile.getInstanceOf("CountTable");
        st.add("tableName", tableName.toUpperCase());
        return st.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String limitRow(int row) {
        return " FETCH FIRST " + row + " ROW ONLY";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createViolationTable(String violationTableName) {
        ST st = groupFile.getInstanceOf("InstallViolationTable");
        st.add("violationTableName", violationTableName.toUpperCase());
        return st.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createRepairTable(String repairTableName) {
        ST st = groupFile.getInstanceOf("InstallRepairTable");
        st.add("repairTableName", repairTableName.toUpperCase());
        return st.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createAuditTable(String auditTableName) {
        ST st = groupFile.getInstanceOf("InstallAuditTable");
        st.add("auditTableName", auditTableName.toUpperCase());
        return st.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String nextVid(String tableName) {
        ST st = groupFile.getInstanceOf("NextVid");
        st.add("tableName", tableName.toUpperCase());
        return st.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String createTableFromCSV(String tableName, String content) {
        ST st = groupFile.getInstanceOf("CreateTableFromCSV");
        st.add("tableName", tableName.toUpperCase());
        st.add("content", content);
        return st.render();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String insertTableFromCSV(
        ResultSetMetaData metaData, String tableName, String row) {
        StringBuilder valueBuilder = new StringBuilder(1024);
        StringBuilder columnBuilder = new StringBuilder(1024);
        String[] tokens = row.split(",");
        try {
            for (int i = 0; i < tokens.length; i ++) {
                // skip column 0 for tid.
                columnBuilder.append(metaData.getColumnName(i + 2));

                int type = metaData.getColumnType(i + 2);
                if (type == Types.VARCHAR || type == Types.CHAR) {
                    valueBuilder.append('\'').append(tokens[i]).append('\'');
                } else {
                    valueBuilder.append(tokens[i]);
                }

                if (i != tokens.length - 1) {
                    valueBuilder.append(',');
                    columnBuilder.append(',');
                }
            }
        } catch (SQLException ex) {
            // type info is missing
            return "Missing SQL types when inserting";
        }

        ST st = groupFile.getInstanceOf("InsertTableFromCSV");
        st.add("tableName", tableName.toUpperCase());
        st.add("columns", columnBuilder.toString());
        st.add("values", valueBuilder.toString());
        return st.render();
    }
}
