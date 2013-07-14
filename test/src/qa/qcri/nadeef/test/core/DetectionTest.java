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

package qa.qcri.nadeef.test.core;

import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.Violations;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.CSVTools;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

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
            CSVTools.dump(conn, TestDataRepository.getLocationData1(), "location", true);
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
        NadeefConfiguration.setAlwaysOverride(true);
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
            List<CleanPlan> cleanPlans = TestDataRepository.getCleanPlan5();
            for (CleanPlan cleanPlan : cleanPlans) {
                CleanExecutor executor = new CleanExecutor(cleanPlan);
                executor.detect();
            }
            verifyViolationResult(104 );
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
