/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import org.junit.*;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.pipeline.Flow;
import qa.qcri.nadeef.core.pipeline.NodeCacheManager;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.CSVDumper;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.Connection;
import java.sql.Statement;

/**
 * Test the repair phase of NADEEF.
 */
public class RepairPhaseTest {
    private RuleBuilder ruleBuilder;
    private String tableName;
    private NodeCacheManager cacheManager;

    @Before
    public void setup() {
        Bootstrap.Start();
        Tracer.setVerbose(true);
        ruleBuilder = new RuleBuilder();
        cacheManager = NodeCacheManager.getInstance();
    }

    @Test
    public void test1() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan3();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            Integer count = (Integer)executor.detect().getDetectOutput();
            Assert.assertEquals(2, count.intValue());

            count = (Integer)executor.repair().getRepairOutput();
            Assert.assertEquals(1, count.intValue());

            count = (Integer)executor.getUpdateOutput();
            Assert.assertEquals(1, count.intValue());
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("exceptions : " + ex.getMessage());
        }
    }

    @Test
    public void test2() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            Integer count = (Integer)executor.detect().getDetectOutput();
            Assert.assertEquals(8, count.intValue());

            count = (Integer)executor.repair().getRepairOutput();
            Assert.assertEquals(4, count.intValue());

            count = (Integer)executor.getUpdateOutput();
            Assert.assertEquals(2, count.intValue());
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("exceptions : " + ex.getMessage());
        }
    }
}
