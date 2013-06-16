/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means “Clean” in Arabic
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
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.Violation;

import java.io.*;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

/**
 * Violation Extension helper.
 */
public class Violations {
    /**
     * Generates violation row number from the database.
     * @return new unique violation id.
     */
    public static int getViolationRowCount()
        throws
            ClassNotFoundException,
            SQLException,
            InstantiationException,
            IllegalAccessException {
        Connection conn = DBConnectionFactory.getNadeefConnection();
        String tableName = NadeefConfiguration.getViolationTableName();
        Statement stat = conn.createStatement();
        ResultSet resultSet =
            stat.executeQuery("SELECT COUNT(*) as count from " + tableName);
        int result = -1;
        if (resultSet.next()) {
            result = resultSet.getInt("count");
        } else {
            result = 0;
        }
        stat.close();
        conn.close();
        return result;
    }

    /**
     * Generates violation id from the database.
     * @return new unique violation id.
     */
    public static int generateViolationId()
        throws
        ClassNotFoundException,
        SQLException,
        InstantiationException,
        IllegalAccessException {
        Connection conn = DBConnectionFactory.getNadeefConnection();
        Statement stat = conn.createStatement();
        String tableName = NadeefConfiguration.getViolationTableName();
        ResultSet resultSet =
            stat.executeQuery("SELECT MAX(vid) + 1 as vid from " + tableName);
        int result = -1;
        if (resultSet.next()) {
            result = resultSet.getInt("vid");
        }
        stat.close();
        conn.close();
        return result;
    }

    /**
     * Generates a list of violations from a query result.
     * @param resultSet query result.
     * @return a list of violations.
     */
    public static Collection<Violation> fromQuery(ResultSet resultSet)
        throws SQLException {
        Preconditions.checkNotNull(resultSet);
        List<Violation> result = Lists.newArrayList();
        int lastVid = -1;
        Violation violation = null;
        while (resultSet.next()) {
            int vid = resultSet.getInt("vid");
            String rid = resultSet.getString("rid");
            String tableName = resultSet.getString("tablename");
            int tupleId = resultSet.getInt("tupleid");
            String attribute = resultSet.getString("attribute");
            String value = resultSet.getString("value");
            Column column = new Column(tableName, attribute);
            Cell cell = new Cell(column, tupleId, value);
            if (vid != lastVid || vid == -1) {
                violation = new Violation(rid, vid);
                violation.addCell(cell);
                result.add(violation);
                lastVid = vid;
            } else {
                violation.addCell(cell);
            }
        }
        return result;
    }

    public static Collection<Violation> fromCSV(File csvFile)
        throws IOException {
        Preconditions.checkNotNull(csvFile);
        BufferedReader reader = new BufferedReader(new FileReader(csvFile));
        // skip the head
        String line = reader.readLine();
        List<Violation> result = Lists.newArrayList();
        int lastVid = -1;
        Violation violation = null;
        while ((line = reader.readLine()) != null) {
            String[] token = line.split(";");
            if (token.length != 6) {
                throw
                    new InvalidObjectException("The given CSV is not a valid violation CSV file.");
            }
            int vid = Integer.parseInt(token[0]);
            String rid = token[1].replace("\"", "");
            String tableName = token[2].replace("\"", "");
            int tupleId = Integer.parseInt(token[3]);
            String attribute = token[4].replace("\"", "");
            String value = token[5].replace("\"", "");
            Column column = new Column(tableName, attribute);
            Cell cell = new Cell(column, tupleId, value);
            if (vid != lastVid || vid == -1) {
                violation = new Violation(rid);
                violation.addCell(cell);
                result.add(violation);
                lastVid = vid;
            } else {
                violation.addCell(cell);
            }
        }
        return result;
    }
}
