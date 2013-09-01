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

package qa.qcri.nadeef.core.util;

import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.SQLTable;
import qa.qcri.nadeef.core.datamodel.Schema;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.SQLDialect;
import qa.qcri.nadeef.tools.SqlQueryBuilder;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.*;

/**
 * An utility class for getting meta data from database.
 */
public final class DBMetaDataTool {
    /**
     * Copies the table within the database.
     * @param dbConfig working database config.
     * @param sourceTableName source table name.
     * @param targetTableName target table name.
     */
    public static void copy(
        DBConfig dbConfig,
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
            conn = DBConnectionFactory.createConnection(dbConfig);
            stat = conn.createStatement();
            stat.execute("DROP TABLE IF EXISTS " + targetTableName + " CASCADE");
            stat.execute("SELECT * INTO " + targetTableName + " FROM " + sourceTableName);

            conn.commit();
            resultSet =
                stat.executeQuery(
                "select * from information_schema.columns where table_name = " +
                '\'' + targetTableName +
                "\' and column_name = \'tid\'"
            );
            conn.commit();

            if (!resultSet.next()) {
                stat.execute("alter table " + targetTableName + " add column tid serial primary key");
            }
            conn.commit();
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

        Tracer tracer = Tracer.getTracer(DBMetaDataTool.class);
        Connection conn = null;
        Statement stat = null;
        ResultSet resultSet = null;
        Schema result = null;

        try {
            SqlQueryBuilder builder = new SqlQueryBuilder();
            builder.addFrom(tableName);
            builder.setLimit(1);
            String sql = builder.build();

            conn = DBConnectionFactory.createConnection(config);
            stat = conn.createStatement();

            resultSet = stat.executeQuery(sql);
            ResultSetMetaData metaData = resultSet.getMetaData();
            int count = metaData.getColumnCount();
            Column[] columns = new Column[count];
            for (int i = 1; i <= count; i ++) {
                String attributeName = metaData.getColumnName(i);
                columns[i - 1] = new Column(tableName, attributeName);
            }

            result = new Schema(tableName, columns);
        } catch (Exception ex) {
            tracer.err("Cannot get valid schema.", ex);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (Exception ex) {}
            }

            if (stat != null) {
                try {
                    stat.close();
                } catch (Exception ex) {};
            }

            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
        return result;
    }

    /**
     * Returns <code>True</code> when the given table exists in the connection.
     * @param tableName table name.
     * @return <code>True</code> when the given table exists in the connection.
     */
    public static boolean isTableExist(DBConfig dbConfig, String tableName)
        throws Exception {
        Connection conn = null;
        ResultSet resultSet = null;
        try {
            conn = DBConnectionFactory.createConnection(dbConfig);
            DatabaseMetaData meta = conn.getMetaData();
            resultSet = meta.getTables(null, null, tableName, null);
            return resultSet.next();
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    /**
     * Returns instance of dialect manager based on input dialect.
     * @param dialect input dialect.
     * @return dialect manager instance.
     */
    public static ISQLDialectManager getDialectManagerInstance(SQLDialect dialect) {
        ISQLDialectManager result = null;
        switch (dialect) {
            case DERBY:
                result = new DerbyManager();
                break;
            case POSTGRES:
                break;
        }
        return result;
    }

    /**
     * Returns NADEEF dialect manager instance.
     * @return dialect manager instance.
     */
    public static ISQLDialectManager getNadeefDialectManagerInstance() {
        SQLDialect dialect = NadeefConfiguration.getDbConfig().getDialect();
        return getDialectManagerInstance(dialect);
    }
}
