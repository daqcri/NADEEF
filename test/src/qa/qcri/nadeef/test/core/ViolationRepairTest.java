/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means “Clean” in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.test.core;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.Bootstrap;
import qa.qcri.nadeef.core.util.Violations;
import qa.qcri.nadeef.ruleext.FDRuleBuilder;
import qa.qcri.nadeef.test.TestDataRepository;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Violation repair test.
 */
public class ViolationRepairTest {
    private Collection<Violation> violations;
    private FDRuleBuilder ruleBuilder;

    @Before
    public void setup() {
        try {
            Bootstrap.start();
            violations = Violations.fromCSV(TestDataRepository.getViolationTestData1());
        } catch (IOException e) {
            Assert.fail("Setup failed.");
            e.printStackTrace();
        }
        ruleBuilder = new FDRuleBuilder();
    }

    @After
    public void tearDown() {
        Bootstrap.shutdown();
    }

    @Test
    public void FDRepair1() {
        try {
            Schema.Builder builder = new Schema.Builder();
            Schema schema = builder.table("test").column("B").column("A").column("C").build();
            List<Rule> rules =
                (List<Rule>) ruleBuilder.name("fd1")
                    .schema(schema)
                    .table("test")
                    .value("B|A,C")
                    .build();
            Rule rule = rules.get(0);
            List<Fix> allFixes = Lists.newArrayList();
            for (Violation violation : violations) {
                allFixes.addAll(rule.repair(violation));
            }
            Assert.assertEquals(2, allFixes.size());
            Fix fix2 = allFixes.get(0);
            Fix fix1 = allFixes.get(1);
            Cell left1 = fix1.getLeft();
            Cell right1 = fix1.getRight();
            Cell left2 = fix2.getLeft();
            Cell right2 = fix2.getRight();

            Assert.assertEquals("test.c", left1.getColumn().getFullColumnName());
            Assert.assertEquals(8, left1.getTupleId());
            Assert.assertEquals("test.c", right1.getColumn().getFullColumnName());
            Assert.assertEquals(6, right1.getTupleId());

            Assert.assertEquals("test.a", left2.getColumn().getFullColumnName());
            Assert.assertEquals(8, left2.getTupleId());
            Assert.assertEquals("test.a", right2.getColumn().getFullColumnName());
            Assert.assertEquals(6, right2.getTupleId());

        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
