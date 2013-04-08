/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.test;

import junit.framework.Assert;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.ProjectHint;
import qa.qcri.nadeef.core.datamodel.TableAttribute;

/**
 * Unit testing for hint parsing.
 */
public class RuleHintTest {

    @Test
    public void ProjectHintTest1() {
        ProjectHint projectHint = new ProjectHint("a.b, b, aa.bb.ccc");
        TableAttribute[] attributes = projectHint.getAttributes();
        Assert.assertEquals(attributes.length, 3);
        Assert.assertEquals(attributes[0].getAttributeName(), "b");
        Assert.assertEquals(attributes[0].getTableName(), "a");
        Assert.assertEquals(attributes[0].getSchemaName(), null);
        Assert.assertEquals(attributes[1].getAttributeName(), "b");
        Assert.assertEquals(attributes[1].getTableName(), null);
        Assert.assertEquals(attributes[1].getSchemaName(), null);
        Assert.assertEquals(attributes[2].getAttributeName(), "ccc");
        Assert.assertEquals(attributes[2].getTableName(), "bb");
        Assert.assertEquals(attributes[2].getSchemaName(), "aa");
    }

    @Test(expected = IllegalArgumentException.class)
    public void ProjectHintTest2() {
        ProjectHint projectHint = new ProjectHint("a.b, a.b.c.dd");
    }
}
