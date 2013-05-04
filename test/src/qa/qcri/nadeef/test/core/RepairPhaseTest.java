/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.pipeline.Flow;
import qa.qcri.nadeef.core.pipeline.NodeCacheManager;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.test.TestDataRepository;

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
        ruleBuilder = new RuleBuilder();
        cacheManager = NodeCacheManager.getInstance();
    }

    @Test
    public void test1() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan2();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            Flow[] flows = executor.detect();

            String key = flows[0].getLastOutputKey();
            Integer count = (Integer)cacheManager.get(key);
            Assert.assertEquals(84, count.intValue());

            flows = executor.repair();
            key = flows[0].getLastOutputKey();
            count = (Integer)cacheManager.get(key);
            Assert.assertEquals(28, count.intValue());

            executor.apply(cleanPlan.getRules().get(0));
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail("exceptions : " + ex.getMessage());
        }
    }
}
