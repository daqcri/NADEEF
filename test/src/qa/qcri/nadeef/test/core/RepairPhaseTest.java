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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.pipeline.NodeCacheManager;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.Tracer;

/**
 * Test the repair phase of NADEEF.
 */
public class RepairPhaseTest {
    private NodeCacheManager cacheManager;

    @Before
    public void setup() {
        Bootstrap.start();
        Tracer.setVerbose(true);
        Tracer.setInfo(true);
        cacheManager = NodeCacheManager.getInstance();
    }

    @After
    public void teardown() {
        Assert.assertEquals(2, cacheManager.getSize());
        cacheManager.clear();
    }

    @Test
    public void test1() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan3();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            Integer count = (Integer)executor.detect().getDetectOutput();
            Assert.assertEquals(1, count.intValue());

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
            Assert.assertEquals(2, count.intValue());

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
