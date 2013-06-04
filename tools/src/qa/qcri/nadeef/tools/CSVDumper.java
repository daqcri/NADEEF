/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.tools;

import com.google.common.io.Files;
import com.google.common.base.Stopwatch;
import org.postgresql.PGConnection;
import org.postgresql.copy.CopyManager;

import java.io.*;
import java.lang.String;
import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * CSVDumper is a simple tool which dumps CSV data into database with a new created table.
 */
public class CSVDumper {
    private static final int BULKSIZE = 1024;
    private static PushbackReader pushbackReader =
        new PushbackReader(new StringReader(""), 1024 * 1024);

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
        String tableName = dump(conn, file, fileName);
        return tableName;
    }

    /**
     * Dump CSV file content into a specified database. It replaces the table if the table
     * already existed.
     * @param conn JDBC connection.
     * @param file CSV file.
     * @param tableName new created table name.
     *
     * @return new created table name.
     */
    public static String dump(
            Connection conn,
            File file,
            String tableName
    ) throws IllegalAccessException, SQLException, IOException {
        Tracer tracer = Tracer.getTracer(CSVDumper.class);
        Stopwatch stopwatch = new Stopwatch().start();
        String fullTableName = null;

        try {
            if (conn.isClosed()) {
                throw new IllegalAccessException("JDBC connection is already closed.");
            }

            conn.setAutoCommit(false);
            BufferedReader reader = new BufferedReader(new FileReader(file));
            StringBuilder header = new StringBuilder(reader.readLine());
            // TODO: make it other DB compatible
            header.insert(0, "TID SERIAL PRIMARY KEY,");
            fullTableName = "csv_" + tableName;
            Statement stat = conn.createStatement();
            stat.setFetchSize(1024);
            String sql = "DROP TABLE IF EXISTS " + fullTableName + " CASCADE";
            tracer.verbose(sql);
            stat.execute(sql);

            // create the table
            sql = "CREATE TABLE " + fullTableName + "( " + header + ")";
            tracer.verbose(sql);
            stat.execute(sql);
            tracer.info("Successfully created table " + fullTableName);

            // Batch load the data
            StringBuilder sb = new StringBuilder();
            CopyManager copyManager = ((PGConnection)conn).getCopyAPI();

            int lineCount = 0;
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }
                sb.append(lineCount + 1);
                sb.append(',');
                sb.append(line);
                sb.append('\n');
                if (lineCount % BULKSIZE == 0) {
                    pushbackReader.unread(sb.toString().toCharArray());
                    copyManager.copyIn(
                        "COPY " + fullTableName + " FROM STDIN WITH CSV",
                        pushbackReader
                    );
                    sb.delete(0, sb.length());
                }
                lineCount ++;
            }

            pushbackReader.unread(sb.toString().toCharArray());
            copyManager.copyIn("COPY " + fullTableName + " FROM STDIN WITH CSV", pushbackReader);

            conn.commit();
            stat.close();
            tracer.info(
                "Dumped " + lineCount + " rows in " +
                stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms."
            );
            stopwatch.stop();
        } catch (Exception ex) {
            tracer.err("Cannot load file " + file.getName(), ex);
            ex.printStackTrace();
            if (conn != null) {
                PreparedStatement stat =
                    conn.prepareStatement("DROP TABLE IF EXISTS " + tableName);
                stat.execute();
                stat.close();
                conn.commit();
            }
        }
        return fullTableName;
    }
    // </editor-fold>
}
