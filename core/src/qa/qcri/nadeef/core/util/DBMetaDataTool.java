/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means “Clean” in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.util;

import qa.qcri.nadeef.core.datamodel.SQLTable;
import qa.qcri.nadeef.core.datamodel.Schema;
import qa.qcri.nadeef.tools.DBConfig;

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
        SQLTable sqlTupleCollection =
            new SQLTable(tableName, config);
        return sqlTupleCollection.getSchema();
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
}
