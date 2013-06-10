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

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.Tracer;

import java.util.List;

/**
 * CleanExecutor test.
 */
public class CleanExecutorTest {
    @Before
    public void setup() {
        Bootstrap.start();
        Tracer.setVerbose(true);
    }

    @After
    public void teardown() {
        Bootstrap.shutdown();
    }

    public static class DetectThread implements Runnable {
        private CleanExecutor cleanExecutor;
        public DetectThread(CleanExecutor cleanExecutor) {
            this.cleanExecutor = cleanExecutor;
        }

        @Override
        public void run() {
            cleanExecutor.detect();
        }
    }

    public static class RepairThread implements Runnable {
        private CleanExecutor cleanExecutor;
        public RepairThread(CleanExecutor cleanExecutor) {
            this.cleanExecutor = cleanExecutor;
        }

        @Override
        public void run() {
            cleanExecutor.repair();
        }
    }

    @Test
    public void percentageTest() {
        try {
            List<Double> result = Lists.newArrayList();
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan2();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            Thread thread = new Thread(new DetectThread(executor));
            thread.start();
            while (thread.isAlive()) {
                result.add(executor.getDetectPercentage());
                Thread.sleep(30);
            }

            System.out.println("detect");
            for (Double v : result) {
                System.out.println(v);
            }

            Assert.assertEquals(1.0f, executor.getDetectPercentage(), 0.0);

            result.clear();
            thread = new Thread(new RepairThread(executor));
            thread.start();
            while (thread.isAlive()) {
                result.add(executor.getRepairPercentage());
                Thread.sleep(30);
            }

            System.out.println("repair");
            for (Double v : result) {
                System.out.println(v);
            }
            Assert.assertEquals(1.0f, executor.getRepairPercentage(), 0.0);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
