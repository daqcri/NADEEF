/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.test;

import junit.framework.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.FDRule;
import qa.qcri.nadeef.core.datamodel.Tuple;
import qa.qcri.nadeef.core.datamodel.Violation;

import java.io.StringReader;
import java.util.HashSet;
import java.util.Set;

/**
 * Test FDRule methods.
 */
@RunWith(JUnit4.class)
public class FDRuleTest {
    @Test
    public void parseTest() {
        Set lhs = null;
        Set rhs = null;

        FDRule rule1 = new FDRule("FD1", new StringReader("test.A, test.B,|test.C"));
        lhs = rule1.getLhs();
        Assert.assertEquals(2, lhs.size());
        Assert.assertTrue(lhs.contains(new Cell("test.A")));
        Assert.assertTrue(lhs.contains(new Cell("test.B")));
        rhs = rule1.getRhs();
        Assert.assertEquals(1, rhs.size());
        Assert.assertTrue(rhs.contains(new Cell("test.C")));

        FDRule rule2 = new FDRule("FD2", new StringReader("a.ZIP   | a.CITY,  a.STATE "));
        lhs = rule2.getLhs();
        rhs = rule2.getRhs();
        Assert.assertEquals(1, lhs.size());
        Assert.assertTrue(lhs.contains(new Cell("a.ZIP")));

        Assert.assertEquals(2, rhs.size());
        Assert.assertTrue(rhs.contains(new Cell("a.CITY")));
        Assert.assertTrue(rhs.contains(new Cell("a.STATE")));
    }

    @Test
    public void detectTest() {
        final String tableName = "test";

        Tuple[] tuples = new Tuple[4];
        Cell[] cells = new Cell[3];
        cells[0] = new Cell(tableName, "ZIP");
        cells[1] = new Cell(tableName, "CITY");
        cells[2] = new Cell(tableName, "STATE");
        tuples[0] = new Tuple(cells, new String[] {"0001", "Brooklyn", "NY"});
        tuples[1] = new Tuple(cells, new String[] {"0001", "Chengdu", "Sichuan"});
        tuples[2] = new Tuple(cells, new String[] {"1183JV", "Delft", "Raadstad"});
        tuples[3] = new Tuple(cells, new String[] {"1183JV", "Amsterdam", "Raadstad"});

        FDRule rule = new FDRule("FD1", new StringReader("test.ZIP | test.CITY, test.STATE"));
        Violation[] violations = rule.detect(tuples);
        Assert.assertEquals(8, violations.length);
    }

    @Test
    public void detectPairTest() {
        final String tableName = "test";

        Cell[] cells = new Cell[3];
        cells[0] = new Cell(tableName, "ZIP");
        cells[1] = new Cell(tableName, "CITY");
        cells[2] = new Cell(tableName, "STATE");
        Tuple tupleA = new Tuple(cells, new String[] {"0001", "Brooklyn", "NY"});
        Tuple tupleB = new Tuple(cells, new String[] {"0001", "Chengdu", "Sichuan"});

        FDRule rule = new FDRule("FD1", new StringReader("test.ZIP | test.CITY, test.STATE"));
        Violation[] violations = rule.detect(tupleA, tupleB);
        Assert.assertEquals(4, violations.length);
    }
}
