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

/**
 * My custom rule with pair detection.
 */
public class MyRule2 extends PairTupleRule {
    /**
     * Detect rule with one tuple.
     *
     * @param tuples input tuple.
     * @return Violation set.
     */
    @Override
    public Collection<Violation> detect(TuplePair tuples) {
        Tuple left = tuples.getLeft();
        Tuple right = tuples.getRight();

        Column city = new Column(tableNames.get(0), "city");

        Violation violation = new Violation(ruleName);
        if (!left.get(city).equals(right.get(city))) {
            violation.addTuple(left);
			violation.addTuple(right);
        }

        return Lists.newArrayList(violation);
    }

    /**
     * Repair of this rule.
     *
     * @param violation violation input.
     * @return a candidate fix.
     */
    @Override
    public Collection<Fix> repair(Violation violation) {
        return null;
    }

    @Override
    public Collection<Table> horizontalScope(
        Collection<Table> tables
    ) {
        Table collection = tables.iterator().next();
        String tableName = tableNames.get(0);
        collection.filter(SimpleExpression.newEqual(new Column(tableName, "zip"), "1183JV"));
        return tables;
    }

    @Override
    public Collection<Table> verticalScope(
        Collection<Table> tables
    ) {
        Table collection = tables.iterator().next();
        collection.project("city").project("zip");
        return tables;
    }
}
