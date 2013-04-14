/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.test;

import junit.framework.Assert;
import org.jooq.SQLDialect;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.FDRule;
import qa.qcri.nadeef.core.datamodel.Rule;

import java.io.*;

/**
 * CleanPlan unit test
 */
@RunWith(JUnit4.class)
public class CleanPlanTest {
    private String testFileName1 = "input/CleanPlan1.json";

    @Test
    public void createFromJSONTest() {
        try {
            CleanPlan cleanPlan =
                CleanPlan.createCleanPlanFromJSON(new FileReader(testFileName1));
            Assert.assertEquals("localhost", cleanPlan.getSourceUrl());
            Assert.assertEquals("test", cleanPlan.getSourceTableName());
            Assert.assertEquals("tester", cleanPlan.getSourceTableUserName());
            Assert.assertEquals("pass&fail", cleanPlan.getSourceTableUserPassword());
            Assert.assertEquals(SQLDialect.POSTGRES, cleanPlan.getSqlDialect());
            Assert.assertEquals("output", cleanPlan.getTargetTableName());
            Assert.assertEquals(1, cleanPlan.getRules().size());
            Rule rule = cleanPlan.getRules().get(0);
            Assert.assertTrue(rule instanceof FDRule);
        } catch (Exception ex) {
            Assert.fail(ex.getMessage());
        }
    }
}
