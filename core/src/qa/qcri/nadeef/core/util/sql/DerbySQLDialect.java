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

import java.sql.*;

/**
 * Database manager for Apache Derby database.
 */
public class DerbySQLDialect extends SQLDialectBase {
    private static STGroupFile template =
        new STGroupFile(
            "qa*qcri*nadeef*core*util*sql*template*DerbyTemplate.stg".replace(
            "*", "/"
            ), '$', '$');

    /**
     * {@inheritDoc}
     */
    @Override
    protected STGroupFile getTemplate() {
        return template;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void copyTable(Connection conn, String sourceName, String targetName)
            throws SQLException {
        Statement stat = null;
        try {
            stat = conn.createStatement();
            // reconstruct the SQL statement to create a table with auto-increment tid
            ResultSet rs = stat.executeQuery("SELECT * FROM " + sourceName);
            ResultSetMetaData metaData = rs.getMetaData();
            boolean hasTid = false;
            int countNum = metaData.getColumnCount();
            StringBuilder sqlBuilder = new StringBuilder(1024);
            StringBuilder columns = new StringBuilder(1024);

            for (int i = 1; i <= countNum; i ++) {
                String columnName = metaData.getColumnName(i);
                if (i != 1) {
                    sqlBuilder.append(",");
                    columns.append(",");
                }
                sqlBuilder.append(columnName)
                          .append(" ")
                          .append(metaData.getColumnTypeName(i))
                          .append("(")
                          .append(metaData.getColumnDisplaySize(i))
                          .append(") ");
                columns.append(columnName);
                if (columnName.equalsIgnoreCase("tid")) {
                    hasTid = true;
                }
            }

            if (hasTid) {
                stat.execute("CREATE TABLE " + targetName + " AS SELECT * FROM " + sourceName + " WITH NO DATA");
            } else {
                ST st = getTemplate().getInstanceOf("CreateTableFromCSV");
                st.add("tableName", targetName);
                st.add("content", sqlBuilder.toString());
                String sql = st.render();
                stat.execute(sql);
            }
            String sql =
                "INSERT INTO " + targetName + " (" + columns.toString() + ") SELECT * FROM " + sourceName;
            stat.execute(sql);
        } finally {
            if (stat != null) {
                stat.close();
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String countTable(String tableName) {
        ST st = getTemplate().getInstanceOf("CountTable");
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
    public String importFromCSV(ResultSetMetaData metaData, String tableName, String row) {
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

        ST st = getTemplate().getInstanceOf("InsertTableFromCSV");
        st.add("tableName", tableName.toUpperCase());
        st.add("columns", columnBuilder.toString());
        st.add("values", valueBuilder.toString());
        return st.render();
    }
}
