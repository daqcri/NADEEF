/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.test;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import qa.qcri.nadeef.core.operator.CountOperator;
import qa.qcri.nadeef.core.pipeline.Flow;
import qa.qcri.nadeef.core.pipeline.Node;
import qa.qcri.nadeef.core.pipeline.NodeCacheManager;


/**
 * FlowEngine test.
 */
@RunWith(JUnit4.class)
public class FlowTest {

    @Test
    public void SimpleFlowTest() {
        NodeCacheManager cacheManager = NodeCacheManager.getInstance();
        CountOperator countOperator = new CountOperator();
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
}
