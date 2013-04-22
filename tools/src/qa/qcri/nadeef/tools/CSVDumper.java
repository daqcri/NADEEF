/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import com.google.common.io.Files;
import com.google.common.base.Stopwatch;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.lang.String;
import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * CSVDumper is a simple tool which dumps CSV data into database with a new created table.
 */
public class CSVDumper {

    // <editor-fold desc="Public methods">
    /**
     * Dump CSV file content into a database with default schema name and generated table name.
     * @param conn JDBC connection.
     * @param file CSV file.
     * @return new created table name.
     */
    public static String dump(Connection conn, File file)
            throws IllegalAccessException, SQLException, IOException {
        String fileName = Files.getNameWithoutExtension(file.getName());
        String tableName = "csvtable_" + fileName; //  + '_' + System.currentTimeMillis();
        dump(conn, file, tableName, "public");
        return tableName;
    }

    /**
     * Dump CSV file content into a specified database. It replaces the table if the table
     * already existed.
     * @param conn JDBC connection.
     * @param file CSV file.
     * @param tableName new created table name.
     * @param schemaName schema name
     */
    public static void dump(
            Connection conn,
            File file,
            String tableName,
            String schemaName
    ) throws IllegalAccessException, SQLException, IOException {
        Tracer tracer = Tracer.getTracer(CSVDumper.class);
        Stopwatch stopwatch = new Stopwatch().start();
        try {
            if (conn.isClosed()) {
                throw new IllegalAccessException("JDBC connection is already closed.");
            }

            conn.setAutoCommit(false);

            BufferedReader reader = new BufferedReader(new FileReader(file));
            StringBuilder header = new StringBuilder(reader.readLine());
            // TODO: make it other DB compatible
            header.insert(0, "TID SERIAL PRIMARY KEY,");
            String fullTableName = schemaName + "." + tableName;
            Statement stat = conn.createStatement();
            stat.execute("DROP TABLE IF EXISTS " + fullTableName);

            // create the table
            stat.execute("CREATE TABLE " + fullTableName + "( " + header + ")");
            conn.commit();
            tracer.info("Successfully created table " + tableName);

            // Batch load the data
            DatabaseMetaData metaData = conn.getMetaData();
            ResultSet columnSchemas = metaData.getColumns(null, schemaName, tableName, null);

            String line = null;
            int lineCount = 0;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                String sqlInsert =
                    getInsert(line, schemaName, tableName, columnSchemas);
                stat.addBatch(sqlInsert);
                lineCount ++;
            }

            stat.executeBatch();
            stat.close();
            conn.commit();
            tracer.info(
                "Dumped " + lineCount + " rows in " +
                stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms."
            );
        } catch (Exception ex) {
            tracer.err("Cannot load file" + file.getName());
            ex.printStackTrace();
            if (conn != null) {
                PreparedStatement stat =
                    conn.prepareStatement("DROP TABLE IF EXISTS " + tableName);
                stat.execute();
                stat.close();
                conn.commit();
            }
        }
    }
    // </editor-fold>

    // <editor-fold desc="Private helper">
    /**
     * Generates insert statement based on the CSV line.
     */
    private static String getInsert(
            String line, String schema, String tableName, ResultSet columnSchemas
    ) throws SQLException {
        String[] tokens = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        // TODO: consider not using DEFAULT since it is postgres dependent.
        StringBuilder sqlInsert = new StringBuilder(
            "INSERT INTO " + schema + "." + tableName  + " VALUES " + "(DEFAULT, ");

        // skip the SERIAL key
        columnSchemas.next();
        for (int i = 0; i < tokens.length; i ++) {
            if (i != 0) {
                sqlInsert.append(',');
            }

            String token = tokens[i];
            if (token.startsWith("\"") || token.startsWith("\'")) {
                sqlInsert.append(token);
                continue;
            }

            columnSchemas.next();
            String type = columnSchemas.getString("TYPE_NAME");
            if (type.startsWith("varchar")) {
                sqlInsert.append('\'');
                sqlInsert.append(token);
                sqlInsert.append('\'');
            } else {
                sqlInsert.append(token);
            }
        }

        sqlInsert.append(')');
        columnSchemas.beforeFirst();
        return sqlInsert.toString();
    }
    // </editor-fold>
}
