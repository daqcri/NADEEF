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

package qa.qcri.nadeef.test.core;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.Violation;
import qa.qcri.nadeef.core.pipeline.EquivalentClass;
import qa.qcri.nadeef.core.pipeline.FixDecisionMaker;
import qa.qcri.nadeef.test.TestDataRepository;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Test for FixDecisionMakerBase.
 */
public class FixDecisionMakerTest {
    private List<Fix> loadFix(File path) throws IOException {
        List<Fix> result = Lists.newArrayList();
        BufferedReader reader = new BufferedReader(new FileReader(path));
        String line = reader.readLine();
        Fix.Builder fixBuilder = new Fix.Builder().vid(Violation.UnknownId);
        Cell.Builder cellBuilder = new Cell.Builder();
        while ((line = reader.readLine()) != null) {
            String[] tokens = line.split(";");
            Cell leftCell = cellBuilder.tid(Integer.parseInt(tokens[0]))
                .column(tokens[1])
                .value(tokens[2])
                .build();
            Cell rightCell = cellBuilder.tid(Integer.parseInt(tokens[3]))
                .column(tokens[4])
                .value(tokens[5])
                .build();
            Fix fix = fixBuilder.left(leftCell).right(rightCell).build();
            result.add(fix);
        }
        return result;
    }

    @Test
    public void test1() {
        try {
            List<Fix> fixes = loadFix(TestDataRepository.getFixTestData1());
            FixDecisionMaker eq = new EquivalentClass();
            Collection<Fix> result = eq.decide(fixes);
            Assert.assertEquals(3, result.size());
            for (Fix fix : result) {
                Cell left = fix.getLeft();
                String value = fix.getRightValue();
                switch (left.getTupleId()) {
                    case 2:
                    case 1:
                        Assert.assertEquals("a3", value);
                        break;
                    case 6:
                        Assert.assertEquals("b1", value);
                        break;
                    default:
                        Assert.fail("Incorrect changed tuple.");
                        break;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail(e.getMessage());
        }
    }
}
