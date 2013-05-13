/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import org.jooq.SQLDialect;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.test.TestDataRepository;

import java.io.FileReader;
import java.util.List;

/**
 * CleanPlan unit test
 */
@RunWith(JUnit4.class)
public class CleanPlanTest {
    @BeforeClass
    public static void start() {
        Bootstrap.Start();
    }

    @Test
    public void createFromJSONTest() {
        try {
            CleanPlan cleanPlan =
                CleanPlan.createCleanPlanFromJSON(
                    new FileReader(TestDataRepository.getTestFile1())
                );
            DBConfig source = cleanPlan.getSourceDBConfig();
            Assert.assertEquals("localhost/unittest", source.getUrl());
            Assert.assertEquals("tester", source.getUserName());
            Assert.assertEquals("tester", source.getPassword());
            Assert.assertEquals(SQLDialect.POSTGRES, source.getDialect());
            Assert.assertEquals(1, cleanPlan.getRules().size());
            Rule rule = cleanPlan.getRules().get(0);
            List<String> tableNames = rule.getTableNames();
            Assert.assertEquals(1, tableNames.size());
            Assert.assertEquals("location_copy", tableNames.get(0));
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
