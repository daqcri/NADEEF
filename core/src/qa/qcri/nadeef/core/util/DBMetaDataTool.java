/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means â€œCleanâ€� in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import qa.qcri.nadeef.core.datamodel.SQLTable;
import qa.qcri.nadeef.core.datamodel.Schema;
import qa.qcri.nadeef.core.exception.DBNotSupportedException;
import qa.qcri.nadeef.tools.SQLDialect;

/**
 * An utility class for getting meta data from database.
 */
public final class DBMetaDataTool {
    /**
     * Copies the table within the same database.
     * @param sourceTableName source table name.
     * @param targetTableName target table name.
     */
    public static void copy(
    	Connection conn,
    	IDialect dialect,
        String sourceTableName,
        String targetTableName
        
    ) throws
        ClassNotFoundException,
        SQLException,
        InstantiationException,
        IllegalAccessException,
        DBNotSupportedException{
        Statement stat = null;
        ResultSet resultSet = null;
        try {
            dialect.dropTable(conn, targetTableName);
        	dialect.copy(conn, sourceTableName, targetTableName);
            dialect.addColumnAsSerialPrimaryKeyIfNotExists(conn, targetTableName, "tid");
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }

            if (stat != null) {
                stat.close();
            }
        }
    }

    /**
     * Gets the table schema given a database configuration.
     * @param conn database connection.
     * @param tableName table name.
     * @return the table schema given a database configuration.
     */
    public static Schema getSchema(Connection conn,String tableName)
        throws Exception {
        if (!isTableExist(conn,tableName)) {
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
    public static boolean isTableExist(Connection conn, String tableName)
        throws Exception {
        ResultSet resultSet = null;
        try {
            DatabaseMetaData meta = conn.getMetaData();
            resultSet = meta.getTables(null, null, tableName, null);
            return resultSet.next();
        } finally {
            if (resultSet != null) {
                resultSet.close();
            }
        }
    }
}
