/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import org.jooq.SQLDialect;
import org.postgresql.jdbc4.Jdbc4ResultSetMetaData;

import java.sql.Connection;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * A helper for getting the right meta data from different DBs.
 */
public class DBMetaDataTool {
    /**
     * Gets the base column name from a result set meta data.
     * @param dialect SQL dialect.
     * @param metaData metaData resultset.
     * @param i index.
     * @return Column name.
     * @throws SQLException
     */
    public static String getBaseColumnName(SQLDialect dialect, ResultSetMetaData metaData, int i)
            throws SQLException {
        String result = null;
        switch (dialect) {
            default:
            case POSTGRES:
                result = ((Jdbc4ResultSetMetaData) metaData).getBaseColumnName(i);
        }
        return result;
    }

    /**
     * Gets the base table name from a result set meta data.
     * @param dialect SQL dialect.
     * @param metaData metaData resultset.
     * @param i index.
     * @return Column name.
     * @throws SQLException
     */
    public static String getBaseTableName(SQLDialect dialect, ResultSetMetaData metaData, int i)
            throws SQLException {
        String result = null;
        switch (dialect) {
            default:
            case POSTGRES:
                result = ((Jdbc4ResultSetMetaData) metaData).getBaseTableName(i);
        }
        return result;
    }

    public static void addTidColumn(SQLDialect dialect, Connection conn, String tableName)
            throws SQLException {
        Statement stat = conn.createStatement();
        stat.execute("alter table " + tableName + " add column tid serial");
        conn.commit();
    }
}
