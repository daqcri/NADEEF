/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import org.junit.Assert;
import org.jooq.SQLDialect;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.FDRule;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.test.TestDataRepository;

import java.io.*;
import java.util.List;

/**
 * CleanPlan unit test
 */
@RunWith(JUnit4.class)
public class CleanPlanTest {

    @Test
    public void createFromJSONTest() {
        try {
            CleanPlan cleanPlan =
                CleanPlan.createCleanPlanFromJSON(
                    new FileReader(TestDataRepository.getFDFileName())
                );
            Assert.assertEquals("localhost/unittest", cleanPlan.getSourceUrl());
            Assert.assertEquals("tester", cleanPlan.getSourceUserName());
            Assert.assertEquals("tester", cleanPlan.getSourceUserPassword());
            Assert.assertEquals(SQLDialect.POSTGRES, cleanPlan.getSqlDialect());
            Assert.assertEquals("output", cleanPlan.getTargetTableName());
            Assert.assertEquals(1, cleanPlan.getRules().size());
            Rule rule = cleanPlan.getRules().get(0);
            Assert.assertTrue(rule instanceof FDRule);
            List<String> tableNames = rule.getTableNames();
            Assert.assertEquals(1, tableNames.size());
            Assert.assertEquals("location", tableNames.get(0));
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
