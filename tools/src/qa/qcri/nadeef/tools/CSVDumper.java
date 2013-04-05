/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import qa.qcri.nadeef.core.util.Tracer;

import java.io.BufferedReader;
import java.io.FileReader;
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
     * @param fullFileName CSV file name.
     * @param conn JDBC connection.
     * @return new created table name.
     */
    public static synchronized String dump(String fullFileName, Connection conn) {
        String tableName = "CSVtable_" + fullFileName + '_' + System.currentTimeMillis();
        dump(fullFileName, conn, tableName);
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
            Pattern pattern;
            Matcher matcher;

            BufferedReader reader = new BufferedReader(new FileReader(fullFileName));
            pattern = Pattern.compile(",??([a-zA-Z]\w*)\s*,\s*(\W.*\W)\s*");
            String header = reader.readLine();
            matcher = pattern.matcher(header);
            ArrayList<String> schemaBuilder = new ArrayList<>();
            while (matcher.find()) {
                schemaBuilder.add(matcher.group(1) + matcher.group(2));
            }

            // Build up the table creation sql and create the table.
            StringBuilder sqlBuilder =
                    new StringBuilder("CREATE TABLE IF NOT EXIST " + tableName + "( ");

            for (String column : schemaBuilder) {
                sqlBuilder.append(column);
            }

            sqlBuilder.append(" );");

            conn.prepareStatement(sqlBuilder.toString());
            conn.commit();

            // Batch load the data
            Statement statement = conn.createStatement();
            Pattern patternData = Pattern.compile("\w*(\s+)\w*,*");
            while (true) {
                String line = reader.readLine();
                if (line == null) {
                    break;
                }

                if (line.isEmpty()) {
                    continue;
                }

                StringBuilder insertStatement =
                        new StringBuilder("INSERT INTO " + tableName + " VALUES" + " (");
                matcher = patternData.matcher(line);
                int i = 0;
                while (matcher.find()) {
                    if (i != 0) {
                        insertStatement.append(',');
                    }
                    insertStatement.append(matcher.group(1));
                    i ++;
                }
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
