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
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.TuplePair;
import qa.qcri.nadeef.core.datamodel.Violation;
import qa.qcri.nadeef.core.operator.TuplePairIterator;
import qa.qcri.nadeef.core.operator.Deseralizer;
import qa.qcri.nadeef.core.operator.ViolationDetector;
import qa.qcri.nadeef.core.pipeline.Flow;
import qa.qcri.nadeef.core.pipeline.Node;
import qa.qcri.nadeef.core.pipeline.NodeCacheManager;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.test.TestDataRepository;

import java.util.Collection;
import java.util.List;

/**
 * FlowEngine test.
 */
@RunWith(JUnit4.class)
public class FlowTest {
    @Before
    public void setUp() {
        Bootstrap.Start();
    }

    @Test
    public void SimpleFlowTest() {
        NodeCacheManager cacheManager = NodeCacheManager.getInstance();
        CountOperator countOperator = new CountOperator(null);
        cacheManager.put("Input", 0);

        Flow flow = new Flow();
        flow.setInputKey("Input");
        flow.addNode(new Node(countOperator, "No.0"), 0);
        flow.addNode(new Node(countOperator, "No.1"), 1);
        flow.addNode(new Node(countOperator, "No.2"), 2);
        flow.addNode(new Node(countOperator, "No.3"), 3);
        flow.addNode(new Node(countOperator, "No.4"), 4);
        flow.start();
        String resultKey = flow.getLastOutputKey();
        Integer result = (Integer)cacheManager.get(resultKey);

        Assert.assertEquals("Result is not correct", result.longValue(), 5);
        Assert.assertEquals("Cache is not clean", cacheManager.getSize(), 0);
    }

    @Test
    public void SimpleFDRuleTest() {
        try {
            NodeCacheManager cacheManager = NodeCacheManager.getInstance();
            CleanPlan cleanPlan = TestDataRepository.getFDCleanPlan();
            List<Rule> rules = cleanPlan.getRules();
            String inputKey = cacheManager.put(rules.get(0));

            Flow flow = new Flow();
            flow.setInputKey(inputKey);
            flow.addNode(new Node(new Deseralizer(cleanPlan), "1"), 0);
            flow.addNode(new Node(new TuplePairIterator(), "2"), 1);
            flow.addNode(new Node(new ViolationDetector<TuplePair>(rules.get(0)), "3"), 2);
            flow.start();
            String resultKey = flow.getLastOutputKey();
            Collection<Violation> result = (Collection<Violation>)cacheManager.get(resultKey);

            Assert.assertEquals(4, result.size());
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
