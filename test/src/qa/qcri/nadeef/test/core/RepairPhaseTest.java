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
        try {
            Connection conn = DBConnectionFactory.createNadeefConnection();
            CSVDumper.dump(
                conn,
                TestDataRepository.getLocationData1(),
                "location",
                "public"
            );
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void test1() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan3();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            Flow[] flows = executor.detect();

            String key = flows[0].getLastOutputKey();
            Integer count = (Integer)cacheManager.get(key);
            Assert.assertEquals(2, count.intValue());

            flows = executor.repair();
            key = flows[0].getLastOutputKey();
            count = (Integer)cacheManager.get(key);
            Assert.assertEquals(1, count.intValue());

            int result = executor.apply(cleanPlan.getRules().get(0));
            Assert.assertEquals(1, result);
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
            Flow[] flows = executor.detect();

            String key = flows[0].getLastOutputKey();
            Integer count = (Integer)cacheManager.get(key);
            Assert.assertEquals(84, count.intValue());

            flows = executor.repair();
            key = flows[0].getLastOutputKey();
            count = (Integer)cacheManager.get(key);
            Assert.assertEquals(28, count.intValue());

            int result = executor.apply(cleanPlan.getRules().get(0));
            Assert.assertEquals(6, result);
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("exceptions : " + ex.getMessage());
        }
    }
}
