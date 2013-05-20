/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.rulebuilder;

import com.google.common.io.Files;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Schema;
import qa.qcri.nadeef.ruleext.FDRuleBuilder;

import java.io.File;
import java.util.Collection;

/**
 * Test for FD Rule writer.
 */
public class FDRuleBuilderTest {
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
    public void testFDFileGeneration() {
        FDRuleBuilder fdRuleBuilder = new FDRuleBuilder();
        File output = null;
        try {
            Schema.Builder schemaBuilder = new Schema.Builder();
            Schema schema =
                schemaBuilder.table("table").column("A").column("B").column("C").build();
            output = fdRuleBuilder
                .name("Test")
                .table("table")
                .schema(schema)
                .out(workingDirectory)
                .value("A|B, C")
                .compile().iterator().next();
            System.out.println("Write file in " + output.getAbsolutePath());
            Assert.assertTrue(output.exists());

            schema =
                schemaBuilder
                    .reset()
                    .table("table")
                    .column("D")
                    .column("E")
                    .column("F")
                    .column("GG").build();
            output = fdRuleBuilder
                .table("table")
                .schema(schema)
                .out(workingDirectory)
                .value("D,E|F, GG")
                .compile().iterator().next();
            System.out.println("Write file in " + output.getAbsolutePath());
            Assert.assertTrue(output.exists());
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }

    @Test
    public void testFDLoad() {
        FDRuleBuilder fdRuleBuilder = new FDRuleBuilder();
        Collection<Rule> output = null;
        try {
            Schema.Builder schemaBuilder = new Schema.Builder();
            Schema schema =
                schemaBuilder.table("table").column("A").column("B").column("C").build();
            output = fdRuleBuilder
                .name("Test")
                .schema(schema)
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
    public void testFDLoad2() {
        FDRuleBuilder fdRuleBuilder = new FDRuleBuilder();
        Collection<Rule> output = null;
        try {
            Schema.Builder schemaBuilder = new Schema.Builder();
            Schema schema =
                schemaBuilder.table("table").column("A").column("B").column("C").build();
            output = fdRuleBuilder
                .table("table")
                .schema(schema)
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
