/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.util;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import qa.qcri.nadeef.core.util.sql.DBConnectionPool;
import qa.qcri.nadeef.core.util.sql.SQLDialectBase;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * CSVTools is a simple tool which dumps CSV data into database given a table name.
 *
 * @author Si Yin <siyin@qf.org.qa>
 */
public class CSVTools {
    // <editor-fold desc="Public methods">

    /**
     * Reads the content from CSV file.
     * @param file CSV file.
     * @param separator separator.
     * @return a list of tokens (the header line is skipped).
     */
    public static List<String[]> read(File file, String separator) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file));
        List<String[]> result = Lists.newArrayList();
        String line;
        int count = 0;
        while ((line = reader.readLine()) != null) {
            if (Strings.isNullOrEmpty(line)) {
                continue;
            }

            count ++;
            // skip the header
            if (count == 1) {
                continue;
            }

            String[] tokens = line.split(separator);
            result.add(tokens);
        }
        return result;
    }

    /**
     * Dumps CSV file content into a database with default schema name and generated table name.
     * @param dbConfig DB connection config.
     * @param dialectManager SQL dialect manager.
     * @param file CSV file.
     * @return new created table name.
     */
    public static String dump(DBConfig dbConfig, SQLDialectBase dialectManager, File file)
            throws IllegalAccessException, SQLException, IOException {
        String fileName = Files.getNameWithoutExtension(file.getName());
        String tableName = dump(dbConfig, dialectManager, file, fileName, true);
        return tableName;
    }

    /**
     * Dumps CSV file content into a specified database. It replaces the table if the table
     * already existed.
     * @param dbConfig JDBC connection config.
     * @param file CSV file.
     * @param dialectManager SQL dialect manager.
     * @param tableName new created table name.
     * @param overwrite it overwrites existing table if it exists.
     *
     * @return new created table name.
     */
    public static String dump(
            DBConfig dbConfig,
            SQLDialectBase dialectManager,
            File file,
            String tableName,
            boolean overwrite
    ) throws SQLException {
        Preconditions.checkNotNull(dbConfig);
        Preconditions.checkNotNull(dialectManager);

        Tracer tracer = Tracer.getTracer(CSVTools.class);
        Stopwatch stopwatch = new Stopwatch().start();
        String fullTableName = null;
        Statement stat = null;
        String sql;
        Connection conn = null;
        try {
            conn = DBConnectionPool.createConnection(dbConfig);
            conn.setAutoCommit(false);
            stat = conn.createStatement();
            stat.setFetchSize(1024);

            // overwrites existing tables if necessary
            fullTableName = "CSV_" + tableName;

            DatabaseMetaData meta = conn.getMetaData();
            boolean hasTableExist =
                meta.getTables(null, null, fullTableName.toLowerCase(), null).next() ||
                meta.getTables(null, null, fullTableName.toUpperCase(), null).next();;

            if (hasTableExist) {
                if (!overwrite) {
                    tracer.info(
                        "Found table " + fullTableName + " exists and choose not to overwrite."
                    );
                    return fullTableName;
                } else {
                    stat = conn.createStatement();
                    sql = dialectManager.dropTable(fullTableName);
                    tracer.verbose(sql);
                    stat.execute(sql);
                    conn.commit();
                }
            }

            BufferedReader reader = new BufferedReader(new FileReader(file));
            // TODO: check whether the header exists.
            String line = reader.readLine();
            sql = dialectManager.createTableFromCSV(fullTableName, line);
            tracer.verbose(sql);
            stat.execute(sql);
            conn.commit();
            tracer.info("Successfully created table " + fullTableName);

            ResultSet rs = stat.executeQuery(dialectManager.selectAll(fullTableName));
            ResultSetMetaData metaData = rs.getMetaData();

            // Batch load the data
            int lineCount = 0;
            while ((line = reader.readLine()) != null) {
                if (Strings.isNullOrEmpty(line)) {
                    continue;
                }

                lineCount ++;
                sql = dialectManager.importFromCSV(metaData, fullTableName, line);
                stat.addBatch(sql);
            }

            stat.executeBatch();
            conn.commit();
            tracer.info(
                "Dumped " + lineCount + " rows in " +
                stopwatch.elapsed(TimeUnit.MILLISECONDS) + " ms."
            );
            stopwatch.stop();
        } catch (Exception ex) {
            tracer.err("Cannot load file " + file.getName(), ex);
            if (conn != null) {
                sql = dialectManager.dropTable(fullTableName);
                stat.execute(sql);
                conn.commit();
            }
        } finally {
            if (stat != null) {
                stat.close();
            }

            if (conn != null) {
                conn.close();
            }
        }
        return fullTableName;
    }
    // </editor-fold>
}
