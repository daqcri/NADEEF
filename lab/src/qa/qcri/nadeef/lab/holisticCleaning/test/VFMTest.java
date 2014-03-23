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

package qa.qcri.nadeef.lab.holisticCleaning.test;

import com.google.common.collect.Sets;
import junit.framework.Assert;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.Operation;
import qa.qcri.nadeef.lab.holisticCleaning.VFMSolver;

import java.util.HashSet;
import java.util.List;

public class VFMTest {
    @Test
    public void simpleTest() {
        // simulate a counting test
        // with
        //  10 x t.a = 1
        //   5 x t.a = 2
        //  12 x t.a = t.b
        //   1 x t.b = 3
        // we expect that the result has
        //   t.a = t.b = 3
        HashSet<Fix> fixes = Sets.newHashSet();

        Fix.Builder builder = new Fix.Builder();
        Cell ta = new Cell(new Column("T", "A"), 1, 2);
        for (int i = 0; i < 12; i ++) {
            Cell tb = new Cell(new Column("T", "B"), i, 3);
            fixes.add(builder.left(ta).op(Operation.EQ).right(tb).build());
        }

        for (int i = 0; i < 10; i ++)
            fixes.add(builder.left(ta).op(Operation.EQ).right(1).build());

        for (int i = 0; i < 5; i ++)
            fixes.add(builder.left(ta).op(Operation.EQ).right(2).build());

        List<Fix> result = new VFMSolver().solve(fixes);

        Assert.assertEquals(1, result.size());
        Fix fix = result.get(0);
        Assert.assertTrue(fix.isConstantAssign());

        int value = Integer.parseInt(fix.getRightValue());
        Assert.assertEquals(3, value);
    }
}
