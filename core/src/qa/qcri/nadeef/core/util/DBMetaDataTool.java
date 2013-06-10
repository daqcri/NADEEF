/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package qa.qcri.nadeef.core.util;

import qa.qcri.nadeef.core.datamodel.SQLTable;
import qa.qcri.nadeef.core.datamodel.Schema;

import java.sql.*;

/**
 * An utility class for getting meta data from database.
 */
public final class DBMetaDataTool {
    /**
     * Copies the table within the source database.
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
        Connection conn = null;
        Statement stat = null;
        try {
            conn = DBConnectionFactory.getSourceConnection();
            stat = conn.createStatement();
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
        } finally {
            if (stat != null) {
                stat.close();
            }

            if (conn != null) {
                conn.close();
            }
        }
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
                conn.close();
            }
        }
    }
}
