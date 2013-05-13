/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.rulewriter;

import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.ruleext.FDRuleBuilder;

import java.io.File;
import java.util.Collection;

/**
 * Test for FD Rule writer.
 */
public class FDRuleWriterTest {
    private static File workingDirectory;

    @BeforeClass
    public static void setup() {
        workingDirectory = Files.createTempDir();
    }

    @AfterClass
    public static void tearDown() {
        // remove all the temp files.
        workingDirectory.delete();
    }

    @Test
    public void testFileGeneration() {
        FDRuleBuilder fdRuleBuilder = new FDRuleBuilder();
        File output = null;
        try {
            output = fdRuleBuilder
                .name("Test")
                .table("table")
                .out(workingDirectory)
                .value("A|B, C")
                .compile();
            System.out.println("Write file in " + output.getAbsolutePath());
            Assert.assertTrue(output.exists());

            output = fdRuleBuilder
                .table("table")
                .out(workingDirectory)
                .value("D,E|F, GG")
                .compile();
            System.out.println("Write file in " + output.getAbsolutePath());
            Assert.assertTrue(output.exists());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testLoad() {
        FDRuleBuilder fdRuleBuilder = new FDRuleBuilder();
        Collection<Rule> output = null;
        try {
            output = fdRuleBuilder
                .name("Test")
                .table("table")
                .value("A|B, C")
                .build();
            Assert.assertTrue(output.size() == 1);
            Assert.assertTrue(output.iterator().next() != null);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testLoad2() {
        FDRuleBuilder fdRuleBuilder = new FDRuleBuilder();
        Collection<Rule> output = null;
        try {
            output = fdRuleBuilder
                .table("table")
                .value("A|B, C")
                .build();
            Assert.assertTrue(output.size() == 1);
            Assert.assertTrue(output.iterator().next() != null);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
