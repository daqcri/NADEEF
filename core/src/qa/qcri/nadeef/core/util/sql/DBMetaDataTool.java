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

import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.Schema;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.*;

/**
 * An utility class for getting meta data from database.
 *
 *
 */
public final class DBMetaDataTool {
    /**
     * Copies the table within the database.
     * @param dbConfig working database config.
     * @param dialectManager Dialect manager.
     * @param sourceTableName source table name.
     * @param targetTableName target table name.
     */
    public static void copy(
        DBConfig dbConfig,
        SQLDialectBase dialectManager,
        String sourceTableName,
        String targetTableName
    ) throws
        ClassNotFoundException,
        SQLException,
        InstantiationException,
        IllegalAccessException {
        Connection conn = null;
        Statement stat = null;
        ResultSet resultSet = null;
        try {
            conn = DBConnectionPool.createConnection(dbConfig, true);
            stat = conn.createStatement();
            if (isTableExist(dbConfig, targetTableName)) {
                stat.execute(dialectManager.dropTable(targetTableName));
            }
            dialectManager.copyTable(conn, sourceTableName, targetTableName);
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }

            if (stat != null) {
                stat.close();
            }

            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * Gets the table schema from source database.
     * @param tableName table name.
     * @return the table schema given a database configuration.
     */
    public static Schema getSchema(DBConfig config, String tableName)
        throws Exception {
        if (!isTableExist(config, tableName)) {
            throw new IllegalArgumentException("Unknown table name " + tableName);
        }

        SQLDialectBase dialectManager =
            SQLDialectFactory.getDialectManagerInstance(config.getDialect());
        Tracer tracer = Tracer.getTracer(DBMetaDataTool.class);
        Connection conn = null;
        Statement stat = null;
        ResultSet resultSet = null;
        Schema result = null;

        try {
            SQLQueryBuilder builder = new SQLQueryBuilder();
            builder.addFrom(tableName);
            builder.setLimit(1);
            String sql = builder.build(dialectManager);

            conn = DBConnectionPool.createConnection(config);
            stat = conn.createStatement();

            resultSet = stat.executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            Column[] columns = new Column[count];
            int[] types = new int[count];
            for (int i = 1; i <= count; i ++) {
                String attributeName = metaData.getColumnName(i);
                types[i - 1] = metaData.getColumnType(i);
                columns[i - 1] = new Column(tableName, attributeName);
            }

            result = new Schema(tableName, columns, types);
        } catch (Exception ex) {
            tracer.err("Cannot get valid schema.", ex);
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }

                if (stat != null) {
                    stat.close();
                }

                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException ex) {}
        }
        return result;
    }

    /**
     * Returns <code>True</code> when the given table exists in the connection.
     * @param tableName table name.
     * @return <code>True</code> when the given table exists in the connection.
     */
    public static boolean isTableExist(DBConfig dbConfig, String tableName)
            throws
                SQLException,
                IllegalAccessException,
                InstantiationException,
                ClassNotFoundException {
        Connection conn = null;
        try {
            conn = DBConnectionPool.createConnection(dbConfig, true);
            DatabaseMetaData meta = conn.getMetaData();
            boolean result =
                meta.getTables(null, null, tableName.toUpperCase(), null).next() ||
                meta.getTables(null, null, tableName.toLowerCase(), null).next();
            return result;
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
    }
}
