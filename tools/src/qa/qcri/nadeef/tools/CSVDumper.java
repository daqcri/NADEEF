/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import qa.qcri.nadeef.core.util.Tracer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.String;
import java.sql.*;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * CSVDumper is a simple tool which dumps CSV data into database with a new created table.
 */
public class CSVDumper {

    // <editor-fold desc="Public methods">
    /**
     * Dump CSV file content into a database with default schema name and generated table name.
     * @param conn JDBC connection.
     * @param fullFileName CSV file name.
     * @return new created table name.
     */
    public static String dump(Connection conn, String fullFileName)
            throws IllegalAccessException, SQLException, IOException {
        File file = new File(fullFileName);
        int fileNameIndex = file.getName().indexOf('.');
        String fileName = file.getName().substring(0, fileNameIndex);
        String tableName = "csvtable_" + fileName + '_' + System.currentTimeMillis();
        dump(conn, fullFileName, tableName, "public");
        return tableName;
    }

    /**
     * Dump CSV file content into a specified database. It replaces the table if the table
     * already existed.
     * @param conn JDBC connection.
     * @param fullFileName CSV file.
     * @param tableName new created table name.
     * @param schemaName schema name
     */
    public static void dump(
            Connection conn,
            String fullFileName,
            String tableName,
            String schemaName
    ) throws IllegalAccessException, SQLException, IOException {
        Tracer tracer = Tracer.getTracer(CSVDumper.class);
        try {
            if (conn.isClosed()) {
                throw new IllegalAccessException("JDBC connection is already closed.");
            }

            conn.setAutoCommit(false);

            BufferedReader reader = new BufferedReader(new FileReader(fullFileName));
            String header = reader.readLine();

            // create the table
            PreparedStatement createStat =
                    conn.prepareStatement(
                            "CREATE TABLE IF NOT EXISTS " + schemaName + "." + tableName +
                            "( " + header + ")"
                    );
            createStat.execute();
            conn.commit();
            tracer.info("Successfully created table " + tableName);

            // Batch load the data
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columnSchemas = metaData.getColumns(null, schemaName, tableName, null);

            Statement stat = conn.createStatement();
            String line = null;
            int lineCount = 0;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String sqlInsert = getInsert(line, schemaName, tableName, columnSchemas);
                stat.addBatch(sqlInsert);
                lineCount ++;
            }

            stat.executeBatch();
            stat.close();
            conn.commit();
            tracer.info("Dumped " + lineCount + " rows");
        } catch (Exception ex) {
            tracer.err("Cannot load file" + fullFileName);
            ex.printStackTrace();
            if (conn != null) {
                PreparedStatement stat =
                        conn.prepareStatement("DROP TABLE IF EXISTS " + tableName);
                stat.execute();
                stat.close();
                conn.commit();
            }
            throw ex;
        }
    }
    // </editor-fold>

    // <editor-fold desc="Private helper">
    /**
     * Generates insert statement based on the CSV line.
     * @param line
     * @param tableName
     * @param columnSchemas
     * @return
     * @throws SQLException
     */
    private static String getInsert(
            String line, String schema, String tableName, ResultSet columnSchemas
    ) throws SQLException {
        String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        StringBuilder sqlInsert = new StringBuilder(
                "INSERT INTO " + schema + "." + tableName  + " VALUES " + "(");
        int i = 0;
        while (columnSchemas.next()) {
            if (i != 0) {
                sqlInsert.append(',');
            }

            String token = tokens[i];
            if (token.startsWith("\"") || token.startsWith("\'")) {
                sqlInsert.append(token);
                continue;
            }

            String type = columnSchemas.getString("TYPE_NAME");
            if (type.startsWith("varchar")) {
                sqlInsert.append('\'');
                sqlInsert.append(token);
                sqlInsert.append('\'');
            } else {
                sqlInsert.append(token);
            }

            i ++;
        }

        sqlInsert.append(')');
        columnSchemas.beforeFirst();
        return sqlInsert.toString();
    }
    // </editor-fold>
}
