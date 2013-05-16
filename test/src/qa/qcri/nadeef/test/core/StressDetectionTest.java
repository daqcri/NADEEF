/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.Violations;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.*;
import java.util.List;

/**
 * Stress testing on detection.
 */
@RunWith(JUnit4.class)
public class StressDetectionTest {
    @Before
    public void setUp() {
        Bootstrap.Start();
        Tracer.setVerbose(false);
    }

    @Test
    public void cleanExecutorTest10k() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getStressPlan10k();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            List<String> tableNames = cleanPlan.getRules().get(0).getTableNames();
            int correctResult =
                getViolationCount(
                    cleanPlan.getSourceDBConfig(),
                    tableNames.get(0),
                    "zipcode",
                    "city"
                );
            executor.detect();
            verifyViolationResult(correctResult);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void cleanExecutorTest30k() {
        Tracer.setVerbose(false);
        Tracer.setInfo(false);
        try {
            CleanPlan cleanPlan = TestDataRepository.getStressPlan30k();
            List<String> tableNames = cleanPlan.getRules().get(0).getTableNames();
            int correctResult =
                getViolationCount(
                    cleanPlan.getSourceDBConfig(),
                    tableNames.get(0),
                    "zipcode",
                    "city"
                );
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.detect();
            verifyViolationResult(correctResult);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private int getViolationCount(
        DBConfig dbConfig,
        String tableName,
        String left,
        String right
    ) {
        Connection conn = null;
        int totalViolation = 0;
        try {
            conn = DBConnectionFactory.createConnection(dbConfig);
            PreparedStatement ps1 =
                conn.prepareStatement(
                    "select distinct " + right + " from " + tableName + " where " + left + " = ?"
                );

            PreparedStatement ps2 = conn.prepareStatement(
                "select count(*) as count from " + tableName + " where " + left + " = ? and " +
                    right + " = ?"
            );

            PreparedStatement ps3 =
                conn.prepareStatement(
                    "select distinct(" + left + ") from " + tableName
                );

            ResultSet result = ps3.executeQuery();
            while (result.next()) {
                String lvalue = result.getString(left);
                ps1.setString(1, lvalue);
                ps2.setString(1, lvalue);
                ResultSet countRs = ps1.executeQuery();
                int count = 0;
                int vcount = 1;
                while (countRs.next()) {
                    // calc. the violations
                    String rvalue = countRs.getString(right);
                    ps2.setString(2, rvalue);
                    ResultSet vcountSet = ps2.executeQuery();
                    if (vcountSet.next()) {
                        vcount *= vcountSet.getInt("count");
                    }
                    count ++;
                }

                if (count > 1) {
                    totalViolation += vcount;
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    //ignore
                }
            }
        }

        return totalViolation * 2;
    }

    private void verifyViolationResult(int expectRow)
        throws
        ClassNotFoundException,
        SQLException,
        InstantiationException,
        IllegalAccessException {
        int rowCount = Violations.getViolationRowCount();
        Assert.assertEquals(expectRow, rowCount);
    }
}
