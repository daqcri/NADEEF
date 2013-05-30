/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.Violations;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.CSVDumper;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Tests for CleanExecutor.
 */
@RunWith(JUnit4.class)
public class DetectionTest {
    @BeforeClass
    public static void startup() {
        Bootstrap.start();
        Tracer.setVerbose(true);

        Connection conn = null;
        try {
            conn = DBConnectionFactory.getNadeefConnection();
            CSVDumper.dump(conn, TestDataRepository.getLocationData1(), "location");
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception ex) {
                    // ignore
                }
            }
        }
        Bootstrap.shutdown();
    }

    @Before
    public void setup() {
        Bootstrap.start();
        Tracer.setVerbose(true);
    }

    @After
    public void teardown() {
        Bootstrap.shutdown();
    }

    @Test
    public void cleanExecutorTest() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.detect();
            verifyViolationResult(12);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void cleanExecutorTest2() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan2();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.initialize(cleanPlan);
            executor.detect();
            verifyViolationResult(84);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void cleanExecutorTest3() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan3();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.initialize(cleanPlan);
            executor.detect();
            verifyViolationResult(2);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void cleanExecutorTest4() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan4();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.initialize(cleanPlan);
            executor.detect();
            verifyViolationResult(8);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void cleanExecutorTest5() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan5();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.initialize(cleanPlan);
            executor.detect();
            verifyViolationResult(84);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void cleanExecutorTest6() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getAdultPlan1();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.initialize(cleanPlan);
            executor.detect();
            verifyViolationResult(4);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void cleanExecutorTest7() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getAdultPlan2();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.initialize(cleanPlan);
            executor.detect();
            verifyViolationResult(974);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
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
