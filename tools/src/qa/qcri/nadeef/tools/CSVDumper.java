/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import qa.qcri.nadeef.core.util.Tracer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.String;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * CSVDumper is a simple tool which dumps CSV data into database with a new created table.
 */
public class CSVDumper {

    /**
     * Dump CSV file content into a database.
     * @param conn JDBC connection.
     * @param fullFileName CSV file name.
     * @return new created table name.
     */
    public static synchronized String dump(Connection conn, String fullFileName) {
        String tableName = "CSVtable_" + fullFileName + '_' + System.currentTimeMillis();
        dump(conn, fullFileName, tableName);
        return tableName;
    }

    /**
     * Dump CSV file content into a specified database. It replaces the table if the table
     * already existed.
     * @param conn JDBC connection.
     * @param fullFileName CSV file.
     * @param tableName new created table name.
     */
    public static void dump(Connection conn, String fullFileName, String tableName) {
        try {
            if (conn.isClosed()) {
                throw new IllegalAccessException("JDBC connection is already closed.");
            }

            conn.setAutoCommit(false);

            BufferedReader reader = new BufferedReader(new FileReader(fullFileName));
            String header = reader.readLine();

            // create the table
            String sql = "CREATE TABLE IF NOT EXIST " + tableName + "( " + header + ");";
            conn.prepareStatement(sql);
            conn.commit();

            // Batch load the data
            Statement statement = conn.createStatement();
            String line = null;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }

                String insertStatement = "INSERT INTO " + tableName + " VALUES" + " (" + line + ");";
                statement.addBatch(insertStatement.toString());
            }

            statement.executeBatch();
            statement.close();
        } catch (Exception ex) {
            Tracer tracer = Tracer.getInstance();
            tracer.info("Cannot open file" + fullFileName);
            ex.printStackTrace();
        }
    }
}
