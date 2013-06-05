/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.rulebuilder;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.ruleext.CFDRuleBuilder;

import java.util.Collection;
import java.util.List;

/**
 * Test on CFD Rule.
 */
@RunWith(JUnit4.class)
public class CFDRuleBuilderTest {
    @Test
    public void parseTest() {
        try {
            List<String> value = Lists.newArrayList(
                "A, B,C , D, E|FF, GG, HH, JJ",
                "a1, _, _, _, E2, f2, _, _, j3",
                "_, b1, _, _, _, f3, _, h1, _"
            );
            CFDRuleBuilder ruleBuilder = new CFDRuleBuilder();
            Collection<Rule> rules =
                ruleBuilder.name("cfd1")
                .table("test1")
                .value(value)
                .build();

            Assert.assertEquals(8, rules.size());

            Assert.assertEquals(5, ruleBuilder.getLhs().size());
            Assert.assertEquals(4, ruleBuilder.getRhs().size());
            Assert.assertEquals(2, ruleBuilder.getFilterExpressions().size());
            List<SimpleExpression> filter = ruleBuilder.getFilterExpressions().get(0);
            Assert.assertEquals(9, filter.size());
            filter = ruleBuilder.getFilterExpressions().get(1);
            Assert.assertEquals(9, filter.size());
        } catch (Exception ignore) {
            ignore.printStackTrace();
            Assert.fail(ignore.getMessage());
        }
    }
}
