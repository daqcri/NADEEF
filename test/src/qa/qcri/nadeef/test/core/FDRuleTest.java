/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import qa.qcri.nadeef.core.datamodel.*;

import java.io.StringReader;
import java.util.*;

/**
 * Test FDRule methods.
 */
@RunWith(JUnit4.class)
public class FDRuleTest {
    @Test
    public void parseTest() {
        Set lhs = null;
        Set rhs = null;

        List<String> tables = Arrays.asList("test");
        FDRule rule1 = new FDRule("FD1", tables, new StringReader("test.A, test.B,|test.C"));
        lhs = rule1.getLhs();
        Assert.assertEquals(2, lhs.size());
        Assert.assertTrue(lhs.contains(new Cell("test.A")));
        Assert.assertTrue(lhs.contains(new Cell("test.B")));
        rhs = rule1.getRhs();
        Assert.assertEquals(1, rhs.size());
        Assert.assertTrue(rhs.contains(new Cell("test.C")));
        RuleHintCollection hints = rule1.getHints();
        Assert.assertEquals(2, hints.size(RuleHintType.Project));
        List<RuleHint> projectHints = hints.getHint(RuleHintType.Project);
        Assert.assertEquals(2, projectHints.size());
        // TODO: add test for the attribute inspection

        tables = Arrays.asList("a");
        FDRule rule2 = new FDRule("FD2", tables, new StringReader("a.ZIP   | a.CITY,  a.STATE "));
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

        List<String> tables = Arrays.asList("test");
        FDRule rule =
            new FDRule("FD1", tables, new StringReader("test.ZIP | test.CITY, test.STATE"));
        Collection<Violation> violations = rule.detect(new TuplePair(tuples[0], tuples[1]));
        Assert.assertEquals(4, violations.size());
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

        List<String> tables = Arrays.asList("test");
        FDRule rule =
            new FDRule("FD1", tables, new StringReader("test.ZIP | test.CITY, test.STATE"));
        Collection<Violation> violations = rule.detect(new TuplePair(tupleA, tupleB));
        Assert.assertEquals(4, violations.size());
    }
}
