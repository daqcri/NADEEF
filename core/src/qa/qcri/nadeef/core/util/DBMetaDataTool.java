/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import qa.qcri.nadeef.core.datamodel.SQLTable;
import qa.qcri.nadeef.core.datamodel.Schema;

import java.sql.*;

/**
 * A helper for getting the right meta data from different DBs.
 */
public class DBMetaDataTool {
    /**
     * Copy the table within the source database.
     * @param sourceTableName source table name.
     * @param targetTableName target table name.
     */
    public static void copy(
        String sourceTableName,
        String targetTableName
    ) throws
        ClassNotFoundException,
        SQLException,
        InstantiationException,
        IllegalAccessException {
        Connection conn = DBConnectionFactory.getSourceConnection();
        Statement stat = conn.createStatement();
        stat.execute("DROP TABLE IF EXISTS " + targetTableName + " CASCADE");
        stat.execute("SELECT * INTO " + targetTableName + " FROM " + sourceTableName);

        conn.commit();
        ResultSet resultSet =
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
        stat.close();
        conn.close();
    }

    /**
     * Gets the table schema given a database configuration.
     * @param tableName table name.
     * @return the table schema given a database configuration.
     */
    public static Schema getSchema(String tableName)
        throws Exception {
        if (!isTableExist(tableName)) {
            throw new IllegalArgumentException("Unknown table name " + tableName);
        }
        SQLTable sqlTupleCollection =
            new SQLTable(tableName, DBConnectionFactory.getSourceDBConfig());
        return sqlTupleCollection.getSchema();
    }

    /**
     * Returns <code>True</code> when the given table exists in the connection.
     * @param tableName table name.
     * @return <code>True</code> when the given table exists in the connection.
     */
    public static boolean isTableExist(String tableName)
        throws Exception {
        Connection conn = null;
        try {
            conn = DBConnectionFactory.getSourceConnection();
            DatabaseMetaData meta = conn.getMetaData();
            ResultSet tables = meta.getTables(null, null, tableName, null);
            if (!tables.next()) {
                return false;
            }
            return true;
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // ignore
                }
            }
        }
    }
}
