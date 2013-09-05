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

import org.junit.*;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.exception.InvalidCleanPlanException;
import qa.qcri.nadeef.core.exception.InvalidRuleException;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.sql.DBConnectionFactory;
import qa.qcri.nadeef.core.util.sql.SQLDialectManagerFactory;
import qa.qcri.nadeef.tools.CSVTools;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.test.TestDataRepository;
import qa.qcri.nadeef.tools.SQLDialect;
import qa.qcri.nadeef.tools.Tracer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.sql.Connection;
import java.util.List;

/**
 * CleanPlan unit test
 */
@RunWith(JUnit4.class)
public class CleanPlanTest {
    private static String testConfig =
        "test*src*qa*qcri*nadeef*test*input*config*derbyConfig.conf".replace(
            '*',
            File.separatorChar
        );
    private ExpectedException thrown = ExpectedException.none();

    @Before
    public void setup() {
        Connection conn = null;
        try {
            Bootstrap.start(testConfig);
            Tracer.setVerbose(true);
            conn = DBConnectionFactory.getNadeefConnection();
            CSVTools.dump(
                conn,
                SQLDialectManagerFactory.getNadeefDialectManagerInstance(),
                TestDataRepository.getLocationData1(),
                "location",
                true
            );
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        } finally {
            if (conn != null) {
                try {
                    conn.close();
                } catch (Exception ex) {
                    // ignore
                }
            }
        }
    }

    @After
    public void teardown() {
        Bootstrap.shutdown();
    }

    @Test
    public void createFromJSONTest() {
        try {
            List<CleanPlan> cleanPlans =
                CleanPlan.createCleanPlanFromJSON(
                    new FileReader(TestDataRepository.getTestFile1())
                );

            Assert.assertEquals(1, cleanPlans.size());
            CleanPlan cleanPlan = cleanPlans.get(0);

            DBConfig source = cleanPlan.getSourceDBConfig();
            Assert.assertEquals("jdbc:derby:memory:nadeefdb;create=true", source.getUrl());
            Assert.assertEquals(SQLDialect.DERBY, source.getDialect());
            Rule rule = cleanPlan.getRule();
            List<String> tableNames = rule.getTableNames();
            Assert.assertEquals(1, tableNames.size());
            Assert.assertEquals("location_copy", tableNames.get(0));
        } catch (Exception ex) {
            ex.printStackTrace();
            Assert.fail(ex.getMessage());
        }
    }

    @Test
    public void createFromJSONTest2() {
        thrown.expect(IllegalArgumentException.class);
        try {
            CleanPlan.createCleanPlanFromJSON(
                new FileReader(TestDataRepository.getFailurePlanFile1())
            );
        } catch (InvalidCleanPlanException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (InvalidRuleException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void createFromJSONTest3() {
        thrown.expect(InvalidRuleException.class);
        try {
            CleanPlan.createCleanPlanFromJSON(
                new FileReader(TestDataRepository.getFailurePlanFile2())
            );
        } catch (InvalidCleanPlanException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (InvalidRuleException e) {
            e.printStackTrace();
        }
    }
}
