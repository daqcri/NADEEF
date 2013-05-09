/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.rulewriter;

import org.junit.Test;
import qa.qcri.nadeef.rulewriter.FDWriter;

/**
 * Test for FD Rule writer.
 */
public class FDRuleWriterTest {
    @Test
    public void test1() {
        FDWriter fdWriter = new FDWriter();
        String code =
            fdWriter
                .name("Test")
                .table("table")
                .value("A|B")
                .generate();
        System.out.println(code);
    }
}
