/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import com.google.common.collect.Lists;
import org.junit.*;
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
        Bootstrap.start();
        Tracer.setVerbose(false);
        Tracer.setInfo(false);
    }

    @After
    public void teardown() {
        Tracer.printDetectSummary("");
    }

    @Test
    public void cleanExecutorTest10k() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getStressPlan10k();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.initialize(cleanPlan);
            List<String> tableNames = cleanPlan.getRule().getTableNames();
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
            List<String> tableNames = cleanPlan.getRule().getTableNames();
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
            Tracer.printDetectSummary();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void cleanExecutorTest40k() {
        Tracer.setVerbose(false);
        Tracer.setInfo(false);
        try {
            CleanPlan cleanPlan = TestDataRepository.getStressPlan40k();
            List<String> tableNames = cleanPlan.getRule().getTableNames();
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

            ResultSet rs3 = ps3.executeQuery();
            while (rs3.next()) {
                String lvalue = rs3.getString(left);
                ps1.setString(1, lvalue);
                ps2.setString(1, lvalue);
                ResultSet rs1 = ps1.executeQuery();
                // distinct rhs with same lhs
                List<Integer> distincts = Lists.newArrayList();
                int totalDistinct = 0;
                while (rs1.next()) {
                    // calc. the violations
                    String rvalue = rs1.getString(right);
                    ps2.setString(2, rvalue);
                    ResultSet rs2 = ps2.executeQuery();
                    if (rs2.next()) {
                        distincts.add(rs2.getInt("count"));
                    }
                    totalDistinct += rs2.getInt("count");
                }

                int sum = 0;
                for (int i = 0; i < distincts.size(); i ++) {
                    int csize = distincts.get(i);
                    sum += csize * (totalDistinct - csize);
                    totalDistinct -= csize;
                }

                totalViolation += sum;
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

        return totalViolation * 4;
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
