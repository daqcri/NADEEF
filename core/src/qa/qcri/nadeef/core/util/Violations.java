/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.Tuple;
import qa.qcri.nadeef.core.datamodel.Violation;

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
        Connection conn = DBConnectionFactory.createNadeefConnection();
        Statement stat = conn.createStatement();
        ResultSet resultSet =
            stat.executeQuery("SELECT COUNT(*) as count from violation");
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
        Connection conn = DBConnectionFactory.createNadeefConnection();
        Statement stat = conn.createStatement();
        ResultSet resultSet =
            stat.executeQuery("SELECT MAX(vid) + 1 as vid from violation");
        int result = -1;
        if (resultSet.next()) {
            result = resultSet.getInt("vid");
        }
        stat.close();
        conn.close();
        return result;
    }

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
