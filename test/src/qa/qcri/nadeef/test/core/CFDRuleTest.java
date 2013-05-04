/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import qa.qcri.nadeef.core.datamodel.CFDRule;
import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.RuleType;
import qa.qcri.nadeef.core.datamodel.SimpleExpression;
import qa.qcri.nadeef.core.util.RuleBuilder;

import java.util.List;

/**
 * Test on CFD Rule.
 */
@RunWith(JUnit4.class)
public class CFDRuleTest {
    @Test
    public void parse1() {
        try {
            List<String> value = Lists.newArrayList(
                "A, B,C , D, E|FF, GG, HH, JJ",
                "a1, _, _, _, E2, f2, _, _, j3"
            );
            RuleBuilder ruleBuilder = new RuleBuilder(RuleType.CFD);
            CFDRule rule = (CFDRule)ruleBuilder.name("cfd1")
                .table("test1")
                .value(value)
                .build();

            List<Column> lhs = rule.getLhs();
            List<Column> rhs = rule.getRhs();
            List<SimpleExpression> expressions = rule.getFilterExpressions();
            Assert.assertEquals(5, lhs.size());
            Assert.assertEquals(4, rhs.size());
            Assert.assertEquals(4, expressions.size());
        } catch (Exception ignore) {
            Assert.fail(ignore.getMessage());
            ignore.printStackTrace();
        }
    }
}
