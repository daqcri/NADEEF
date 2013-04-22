/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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
        ResultSet resultSet = stat.executeQuery("SELECT COUNT(*) as count from violation");
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
        ResultSet resultSet = stat.executeQuery("SELECT MAX(vid) + 1 as vid from violation");
        int result = -1;
        if (resultSet.next()) {
            result = resultSet.getInt("vid");
        }
        stat.close();
        conn.close();
        return result;
    }
}
