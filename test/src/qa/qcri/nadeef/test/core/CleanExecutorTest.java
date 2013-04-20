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
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.util.Bootstrap;

import qa.qcri.nadeef.test.TestDataRepository;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

/**
 * Tests for CleanExecutor.
 */
@RunWith(JUnit4.class)
public class CleanExecutorTest {

    @Before
    public void setUp() {
        Bootstrap.Start();
    }

    @Test
    public void cleanExecutorTest() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getFDCleanPlan();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.run();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void cleanExecutorTest2() {
        try {
            CleanPlan cleanPlan = TestDataRepository.getFDCleanPlan2();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.run();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
