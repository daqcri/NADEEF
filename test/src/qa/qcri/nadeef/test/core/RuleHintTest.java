/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import org.junit.Assert;

import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.Column;

import java.util.List;

/**
 * Unit testing for hint parsing.
 */
//public class RuleHintTest {
//
//    @Test
//    public void ProjectHintTest1() {
//        ProjectHint projectHint = new ProjectHint("a.b, b.b, aa.bb.ccc");
//        List<Column> attributes = projectHint.getAttributes();
//        Assert.assertEquals(attributes.size(), 3);
//        Assert.assertEquals(attributes.get(0).getAttributeName(), "b");
//        Assert.assertEquals(attributes.get(0).getTableName(), "a");
//        Assert.assertEquals(attributes.get(0).getSchemaName(), "public");
//        Assert.assertEquals(attributes.get(1).getAttributeName(), "b");
//        Assert.assertEquals(attributes.get(1).getTableName(), "b");
//        Assert.assertEquals(attributes.get(1).getSchemaName(), "public");
//        Assert.assertEquals(attributes.get(2).getAttributeName(), "ccc");
//        Assert.assertEquals(attributes.get(2).getTableName(), "bb");
//        Assert.assertEquals(attributes.get(2).getSchemaName(), "aa");
//    }
//
//    @Test(expected = IllegalArgumentException.class)
//    public void ProjectHintTest2() {
//        ProjectHint projectHint = new ProjectHint("a.b, a.b.c.dd");
//    }
//}
