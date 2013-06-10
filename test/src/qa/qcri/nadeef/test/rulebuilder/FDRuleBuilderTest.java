/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
