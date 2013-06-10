/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
