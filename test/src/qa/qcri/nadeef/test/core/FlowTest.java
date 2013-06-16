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

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import qa.qcri.nadeef.core.pipeline.Flow;
import qa.qcri.nadeef.core.pipeline.Node;
import qa.qcri.nadeef.core.pipeline.NodeCacheManager;
import qa.qcri.nadeef.core.util.Bootstrap;

/**
 * FlowEngine test.
 */
@RunWith(JUnit4.class)
public class FlowTest {
    @Before
    public void setUp() {
        Bootstrap.start();
    }

    @Test
    public void SimpleFlowTest() {
        NodeCacheManager cacheManager = NodeCacheManager.getInstance();
        CountOperator countOperator = new CountOperator(null);
        cacheManager.put("Input", Integer.valueOf(0));

        Flow flow = new Flow("test");
        flow.setInputKey("Input");
        flow.addNode(new Node(countOperator, "No.0"), 0);
        flow.addNode(new Node(countOperator, "No.1"), 1);
        flow.addNode(new Node(countOperator, "No.2"), 2);
        flow.addNode(new Node(countOperator, "No.3"), 3);
        flow.addNode(new Node(countOperator, "No.4"), 4);
        flow.start();
        flow.waitUntilFinish();
        String resultKey = flow.getCurrentOutputKey();
        Integer result = (Integer)cacheManager.get(resultKey);

        Assert.assertEquals("Result is not correct", result.longValue(), 5);
        Assert.assertEquals("Cache is not clean", cacheManager.getSize(), 0);
    }
}
