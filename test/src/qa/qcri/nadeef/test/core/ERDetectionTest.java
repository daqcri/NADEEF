/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.test.core;

import com.google.common.io.Files;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.pipeline.UpdateExecutor;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.Violations;
import qa.qcri.nadeef.core.util.sql.DBInstaller;
import qa.qcri.nadeef.test.NadeefTestBase;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.Tracer;

import java.io.File;

@RunWith(Parameterized.class)
public class ERDetectionTest extends NadeefTestBase {
    private static File workingDirectory;

    public ERDetectionTest(String testConfig_) {
        super(testConfig_);
    }

    @Before
    public void setup() {
        try {
            Bootstrap.start(testConfig);
            workingDirectory = Files.createTempDir();
            DBInstaller.uninstall(NadeefConfiguration.getDbConfig());
            Tracer.setVerbose(true);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }

    @After
    public void tearDown() {
        Bootstrap.shutdown();
        workingDirectory.delete();
    }

    @Test
    public void erTest1() throws Exception {
        try{
            CleanPlan cleanPlan = TestDataRepository.getPlan("ERCleanPlan1.json").get(0);
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.detect();
            verifyViolationResult(51);
        } catch (Exception e){
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void erTest2() throws Exception {
        try{
            CleanPlan cleanPlan = TestDataRepository.getPlan("vehicles2.json").get(0);
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.detect();
            executor.repair();

            UpdateExecutor updateExecutor =
                new UpdateExecutor(cleanPlan, NadeefConfiguration.getDbConfig());
            updateExecutor.run();
        } catch (Exception e){
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void erTest3() throws Exception {
        try{
            CleanPlan cleanPlan = TestDataRepository.getPlan("vehicles3.json").get(0);
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.detect();
            executor.repair();
            UpdateExecutor updateExecutor =
                new UpdateExecutor(cleanPlan, NadeefConfiguration.getDbConfig());
            updateExecutor.run();
        } catch (Exception e){
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void erTest4() throws Exception {
        try{
            CleanPlan cleanPlan = TestDataRepository.getPlan("vehicles4.json").get(0);
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.detect();
            // executor.repair();
            // UpdateExecutor updateExecutor =
            //    new UpdateExecutor(cleanPlan, NadeefConfiguration.getDbConfig());
            // updateExecutor.run();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    private void verifyViolationResult(int expectRow)
        throws Exception {
        int rowCount = Violations.getViolationRowCount(NadeefConfiguration.getDbConfig());
        Assert.assertEquals(expectRow, rowCount);
    }
}