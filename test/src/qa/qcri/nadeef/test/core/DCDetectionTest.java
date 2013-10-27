package qa.qcri.nadeef.test.core;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.Violations;
import qa.qcri.nadeef.core.util.sql.DBConnectionPool;
import qa.qcri.nadeef.core.util.sql.DBInstaller;
import qa.qcri.nadeef.test.NadeefTestBase;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.Tracer;

@RunWith(Parameterized.class)
public class DCDetectionTest extends NadeefTestBase {
    public DCDetectionTest(String testConfig_) {
        super(testConfig_);
    }

    @Before
    public void setup() {
        try {
            Bootstrap.start(testConfig);
            Tracer.setVerbose(true);
            NadeefConfiguration.setMaxIterationNumber(1);
            NadeefConfiguration.setAlwaysOverride(true);
            DBInstaller.uninstall(NadeefConfiguration.getDbConfig());
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @After
    public void teardown() {
        Bootstrap.shutdown();
    }

    @Test
    public void cleanExecutorConstantDCTest(){
        try{
            CleanPlan cleanPlan = TestDataRepository.getConstantDCTestPlan();
            CleanExecutor executor = new CleanExecutor(cleanPlan);
            executor.detect();
            verifyViolationResult(5, executor.getConnectionPool());
        } catch (Exception e){
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void cleanExecutorDCTest(){
        CleanExecutor executor = null;
        try{
            CleanPlan cleanPlan = TestDataRepository.getDCTestPlan();
            executor = new CleanExecutor(cleanPlan);
            executor.detect();
            verifyViolationResult(48, executor.getConnectionPool());
        } catch (Exception e){
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    @Test
    public void cleanExecutorSingleDCTest(){
        CleanExecutor executor = null;
        try{
            CleanPlan cleanPlan = TestDataRepository.getSingleTupleDCTestPlan();
            executor = new CleanExecutor(cleanPlan);
            executor.detect();
            verifyViolationResult(12, executor.getConnectionPool());
        } catch (Exception e){
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    @Test
    public void cleanExecutorFloatDCTest() {
        CleanExecutor executor = null;
        try{
            CleanPlan cleanPlan = TestDataRepository.getFloatDCTestPlan();
            executor = new CleanExecutor(cleanPlan);
            executor.detect();
            verifyViolationResult(24, executor.getConnectionPool());
        } catch(Exception e){
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    @Test
    public void dcGeneratedFileTest() {
        CleanExecutor executor = null;
        try{
            CleanPlan cleanPlan = TestDataRepository.getDCGeneratedCleanPlan();
            executor = new CleanExecutor(cleanPlan);
            executor.detect();
            verifyViolationResult(5, executor.getConnectionPool());
        } catch(Exception e){
            e.printStackTrace();
            Assert.fail(e.getMessage());
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    private void verifyViolationResult(int expectRow, DBConnectionPool pool)
        throws Exception {
        int rowCount = Violations.getViolationRowCount(pool);
        Assert.assertEquals(expectRow, rowCount);
    }
}
