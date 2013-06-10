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

package qa.qcri.nadeef.tools;

import java.sql.*;

/**
 * NADEEF database installation utility class.
 */
public final class DBInstaller {
    /**
     * Checks whether NADEEF is installed in the targeted database connection.
     * @param conn JDBC connection.
     * @param tableName source tableName.
     * @return TRUE when Nadeef is already installed on the database.
     */
    public static boolean isInstalled(Connection conn, String tableName)
            throws SQLException {
        DatabaseMetaData metaData = conn.getMetaData();
        ResultSet resultSet = metaData.getTables(null, null, tableName, null);

        return resultSet.next();
    }

    /**
     * Install NADEEF on the target database.
     */
    // TODO: move connection inside the method.
    public static void install(
        Connection conn,
        String violationTableName,
        String repairTableName,
        String auditTableName) throws SQLException {
        Tracer tracer = Tracer.getTracer(DBInstaller.class);
        if (isInstalled(conn, violationTableName)) {
            tracer.info("Nadeef is installed on the database, please try uninstall first.");
            return;
        }

        Statement stat = conn.createStatement();
        // TODO: make an index key for violations
        stat.execute(
                "CREATE TABLE " +
                violationTableName + " (" +
                "vid int," +
                "rid varchar(255), " +
                "tablename varchar(63), " +
                "tupleid int, " +
                "attribute varchar(63), value text)"
        );

        stat.execute(
            "CREATE TABLE " +
             repairTableName + "(" +
            "id int, " +
            "vid int, " +
            "c1_tupleid int, " +
            "c1_tablename varchar(63), " +
            "c1_attribute varchar(63), " +
            "c1_value text," +
            "op int," +
            "c2_tupleid int, " +
            "c2_tablename varchar(63), " +
            "c2_attribute varchar(63), " +
            "c2_value text)"
        );

        stat.execute(
            "CREATE TABLE " +
            auditTableName + "(" +
            "id serial primary key," +
            "vid int," +
            "tupleid int," +
            "tablename varchar(63)," +
            "attribute varchar(63)," +
            "oldvalue text," +
            "newvalue text," +
            "time timestamp)"
        );

        stat.close();
        conn.commit();
    }

    /**
     * Uninstall NADEEF from the target database.
     * @param conn JDBC Connection.
     */
    public static void uninstall(Connection conn, String tableName)
            throws SQLException {
        if (isInstalled(conn, tableName)) {
            Statement stat = conn.createStatement();
            stat.execute("DROP TABLE " + tableName + " CASCADE");
            stat.close();
            conn.commit();
        }
    }
}
