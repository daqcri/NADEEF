/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.test;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import qa.qcri.nadeef.core.datamodel.FDRule;

import java.io.StringReader;

/**
 * Test FDRule methods.
 */
@RunWith(JUnit4.class)
public class FDRuleTest {
    @Test
    public void parseTest() {
        String[] lhs = null;
        String[] rhs = null;

        FDRule rule1 = new FDRule("FD1", new StringReader("A,B,|C"));
        lhs = rule1.getLhs();
        Assert.assertEquals(2, lhs.length);
        Assert.assertEquals("A", lhs[0]);
        Assert.assertEquals("B", lhs[1]);
        rhs = rule1.getRhs();
        Assert.assertEquals(1, rhs.length);
        Assert.assertEquals("C", rhs[0]);

        FDRule rule2 = new FDRule("FD2", new StringReader("ZIP   | CITY,  STATE "));
        lhs = rule2.getLhs();
        rhs = rule2.getRhs();
        Assert.assertEquals(1, lhs.length);
        Assert.assertEquals("ZIP", lhs[0]);

        Assert.assertEquals(2, rhs.length);
        Assert.assertEquals("CITY", rhs[0]);
        Assert.assertEquals("STATE", rhs[1]);
    }

    @Test
    public void detectTest() {

    }
}
