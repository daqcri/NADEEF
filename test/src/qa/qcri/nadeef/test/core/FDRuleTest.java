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
        Assert.assertTrue(lhs.contains(new Column("test.A")));
        Assert.assertTrue(lhs.contains(new Column("test.B")));
        rhs = rule1.getRhs();
        Assert.assertEquals(1, rhs.size());
        Assert.assertTrue(rhs.contains(new Column("test.C")));
        // TODO: add test for the attribute inspection

        tables = Arrays.asList("a");
        FDRule rule2 = new FDRule("FD2", tables, new StringReader("a.ZIP   | a.CITY,  a.STATE "));
        lhs = rule2.getLhs();
        rhs = rule2.getRhs();
        Assert.assertEquals(1, lhs.size());
        Assert.assertTrue(lhs.contains(new Column("a.ZIP")));

        Assert.assertEquals(2, rhs.size());
        Assert.assertTrue(rhs.contains(new Column("a.CITY")));
        Assert.assertTrue(rhs.contains(new Column("a.STATE")));
    }

    @Test
    public void detectTest() {
        final String tableName = "test";

        Tuple[] tuples = new Tuple[4];
        Column[] columns = new Column[3];
        columns[0] = new Column(tableName, "ZIP");
        columns[1] = new Column(tableName, "CITY");
        columns[2] = new Column(tableName, "STATE");
        tuples[0] = new Tuple(1, columns, new String[] {"0001", "Brooklyn", "NY"});
        tuples[1] = new Tuple(2, columns, new String[] {"0001", "Chengdu", "Sichuan"});
        tuples[2] = new Tuple(3, columns, new String[] {"1183JV", "Delft", "Raadstad"});
        tuples[3] = new Tuple(4, columns, new String[] {"1183JV", "Amsterdam", "Raadstad"});

        List<String> tables = Arrays.asList("test");
        FDRule rule =
            new FDRule("FD1", tables, new StringReader("test.ZIP | test.CITY, test.STATE"));
        Collection<Violation> violations = rule.detect(new TuplePair(tuples[0], tuples[1]));
        Assert.assertEquals(4, violations.size());
    }
}
