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

package qa.qcri.nadeef.test.service;

import org.junit.Assert;
import org.junit.Test;
import org.junit.After;
import org.junit.Before;

import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.service.NadeefJobScheduler;
import qa.qcri.nadeef.service.thrift.TJobStatus;
import qa.qcri.nadeef.service.thrift.TJobStatusType;
import qa.qcri.nadeef.test.TestDataRepository;

import java.net.InetAddress;

/**
 * JobScheduler test.
 */
public class NadeefJobSchedulerTest {
    @Before
    public void setup() {
        Bootstrap.start();
    }

    @After
    public void teardown() {
        Bootstrap.shutdown();
    }

    @Test
    public void test1() {
        try {
            String hostname = InetAddress.getLocalHost().getHostName();
            CleanPlan cleanPlan = TestDataRepository.getCleanPlan();

            NadeefJobScheduler scheduler = NadeefJobScheduler.getInstance();
            String key = scheduler.submitDetectJob(cleanPlan);
            Assert.assertTrue(key.startsWith(hostname));
            System.out.println("key: " + key);
            Thread.sleep(100);
            TJobStatus jobStatus = scheduler.getJobStatus(key);
            Assert.assertEquals(TJobStatusType.RUNNING, jobStatus.getStatus());
            System.out.println("progress: " + jobStatus.getProgress());
            Thread.sleep(2000);
            jobStatus = scheduler.getJobStatus(key);
            Assert.assertEquals(TJobStatusType.NOTAVAILABLE, jobStatus.getStatus());
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail();
        }
    }
}
