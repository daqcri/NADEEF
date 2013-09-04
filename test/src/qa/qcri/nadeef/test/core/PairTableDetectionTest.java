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

package qa.qcri.nadeef.test.core;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.Violations;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.Tracer;

import java.io.File;
import java.sql.SQLException;
import java.util.List;

/**
 * Pair table unit test.
 */
public class PairTableDetectionTest {
    private static String testConfig =
        "test*src*qa*qcri*nadeef*test*input*config*derbyConfig.conf".replace(
                '*', File.separatorChar);
    @Before
    public void setUp() {
        Bootstrap.start(testConfig);
        NadeefConfiguration.setAlwaysOverride(true);
        Tracer.setVerbose(true);
        Tracer.setInfo(true);
    }

    @After
    public void teardown() {
        Tracer.printDetectSummary("");
    }

    @Test
    public void pairCleanPlanTest1() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getPairCleanPlan1();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.detect();
            verifyViolationResult(10);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void verifyViolationResult(int expectRow)
        throws Exception {
        int rowCount = Violations.getViolationRowCount();
        Assert.assertEquals(expectRow, rowCount);
    }
}
