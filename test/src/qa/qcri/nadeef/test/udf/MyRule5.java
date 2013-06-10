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

package qa.qcri.nadeef.test.udf;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;

import java.util.Collection;
import java.util.List;

/**
 * Pair table test.
 */
public class MyRule5 extends PairTupleRule {

    /**
     * Detect rule with pair tuple.
     *
     * @param pair input tuple pair.
     * @return Violation set.
     */
    @Override
    public Collection<Violation> detect(TuplePair pair) {
        List<Violation> result = Lists.newArrayList();
        Tuple bankTuple;
        Tuple tranTuple;
        if (pair.getLeft().isFromTable("bank1")) {
            bankTuple = pair.getLeft();
            tranTuple = pair.getRight();
        } else {
            bankTuple = pair.getRight();
            tranTuple = pair.getLeft();
        }

        if (
            bankTuple.get("FN").equals(tranTuple.get("FN")) &&
            bankTuple.get("LN").equals(tranTuple.get("LN")) &&
            bankTuple.get("ST").equals(tranTuple.get("str")) &&
            bankTuple.get("city").equals(tranTuple.get("city")) &&
            !bankTuple.get("tel").equals(tranTuple.get("phn"))
        ) {
            Violation violation = new Violation(ruleName);
            violation.addCell(bankTuple.getCell("FN"));
            violation.addCell(bankTuple.getCell("LN"));
            violation.addCell(bankTuple.getCell("ST"));
            violation.addCell(bankTuple.getCell("city"));
            violation.addCell(bankTuple.getCell("tel"));
            violation.addCell(tranTuple.getCell("FN"));
            violation.addCell(tranTuple.getCell("LN"));
            violation.addCell(tranTuple.getCell("str"));
            violation.addCell(tranTuple.getCell("city"));
            violation.addCell(tranTuple.getCell("phn"));
            result.add(violation);
        }
        return result;
    }

    /**
     * Repair of this rule.
     *
     * @param violation violation input.
     * @return a candidate fix.
     */
    @Override
    public Collection<Fix> repair(Violation violation) {
        Fix.Builder builder = new Fix.Builder();
        Cell tran = violation.getCell("tran1", "phn");
        Cell bank = violation.getCell("bank1", "tel");
        Fix fix = builder.left(tran).right(bank.getValue().toString()).build();
        return Lists.newArrayList(fix);
    }
}
