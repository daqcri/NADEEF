/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.rulewriter;

import com.google.common.io.Files;
import org.junit.*;
import qa.qcri.nadeef.rulewriter.FDRuleBuilder;
import qa.qcri.nadeef.tools.CommonTools;

import java.io.File;
import java.io.IOException;
import java.net.URL;

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
                .generate();
            System.out.println("Write file in " + output.getAbsolutePath());
            Assert.assertTrue(output.exists());

            output = fdRuleBuilder
                .table("table")
                .out(workingDirectory)
                .value("D,E|F, GG")
                .generate();
            System.out.println("Write file in " + output.getAbsolutePath());
            Assert.assertTrue(output.exists());
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCompiling() {
        FDRuleBuilder fdRuleBuilder = new FDRuleBuilder();
        File output = null;
        try {
            output = fdRuleBuilder
                .name("Test")
                .table("table")
                .value("A|B, C")
                .generate();
            System.out.println("Write file in " + output.getAbsolutePath());
            Assert.assertTrue(output.exists());
            boolean result = CommonTools.compileFile(output);
            Assert.assertTrue(result);
        } catch (IOException e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testCompiling2() {
        FDRuleBuilder fdRuleBuilder = new FDRuleBuilder();
        File output = null;
        try {
            output = fdRuleBuilder
                .table("table")
                .value("A|B, C")
                .generate();
            System.out.println("Write file in " + output.getAbsolutePath());
            Assert.assertTrue(output.exists());
            String outputClass = fdRuleBuilder.compile();
            // URL url = new URL("file://D:\\da\\Projects\\NADEEF\\trunk\\");
            URL url = new URL("file://" + output.getParent() + File.separator);
            Class ruleClass = CommonTools.loadClass(outputClass, url);
            Assert.assertTrue(!Rule.class.isAssignableFrom(ruleClass));
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
