/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.Tuple;
import qa.qcri.nadeef.core.datamodel.Violation;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Violation Extension helper.
 */
public class Violations {

    /**
     * Create violations from a tuple.
     * @param tuple input tuple.
     * @return collection of violations relates to this tuple.
     */
    public static Violation fromTuple(String ruleId, Tuple tuple) {
        List<Column> columns = Lists.newArrayList(tuple.getColumns());
        Violation violation = new Violation(ruleId);
        for (Column column : columns) {
            violation.addCell(tuple, column);
        }
        return violation;
    }

    /**
     * Generates violation id from database.
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
        ResultSet resultSet = stat.executeQuery("SELECT MAX(vid) from violations");
        int result = -1;
        if (resultSet.next()) {
            result = resultSet.getInt("vid");
        }
        return result + 1;
    }
}
