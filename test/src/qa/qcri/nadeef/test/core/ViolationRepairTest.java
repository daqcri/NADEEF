/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Before;

import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.exception.InvalidRuleException;
import qa.qcri.nadeef.core.util.RuleBuilder;
import qa.qcri.nadeef.core.util.Violations;
import qa.qcri.nadeef.test.TestDataRepository;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Violation repair test.
 */
public class ViolationRepairTest {
    private Collection<Violation> violations;
    private RuleBuilder ruleBuilder;

    @Before
    public void setup() {
        try {
            violations = Violations.fromCSV(TestDataRepository.getViolationTestData1());
        } catch (IOException e) {
            Assert.fail("Setup failed.");
            e.printStackTrace();
        }
        ruleBuilder = new RuleBuilder();
    }

    @Test
    public void FDRepair1() {
        try {
            Rule rule =
                ruleBuilder.name("fd1")
                    .type(RuleType.FD)
                    .table("test")
                    .value("B|A,C")
                    .build();

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

            Assert.assertEquals("test.c", left1.getColumn().getFullAttributeName());
            Assert.assertEquals(8, left1.getTupleId());
            Assert.assertEquals("test.c", right1.getColumn().getFullAttributeName());
            Assert.assertEquals(6, right1.getTupleId());

            Assert.assertEquals("test.a", left2.getColumn().getFullAttributeName());
            Assert.assertEquals(8, left2.getTupleId());
            Assert.assertEquals("test.a", right2.getColumn().getFullAttributeName());
            Assert.assertEquals(6, right2.getTupleId());

        } catch (InvalidRuleException e) {
            e.printStackTrace();
        }
    }
}
