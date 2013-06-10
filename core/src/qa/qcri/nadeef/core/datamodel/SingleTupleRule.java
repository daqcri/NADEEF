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

package qa.qcri.nadeef.core.datamodel;

import java.util.Collection;
import java.util.List;

/**
 * SingleTupleRule rule is an abstract class for rule which has detection based on one tuple.
 */
public abstract class SingleTupleRule extends Rule<Tuple> {
    /**
     * Default constructor.
     */
    public SingleTupleRule() {}

    /**
     * Internal method to initialize a rule.
     * @param name Rule name.
     * @param tableNames Table names.
     */
    public void initialize(String name, List<String> tableNames) {
        super.initialize(name, tableNames);
    }

    /**
     * Default scope operation.
     * @param table input tables.
     * @return filtered table.
     */
    @Override
    public Collection<Table> horizontalScope(Collection<Table> table) {
        return table;
    }

    /**
     * Default scope operation.
     * @param table input table.
     * @return filtered table.
     */
    @Override
    public Collection<Table> verticalScope(Collection<Table> table) {
        return table;
    }

    /**
     * Block operator.
     *
     * Current we don't support blocking given multiple tables (co-group). So when a rule
     * is using more than 1 table the block operator is going to be ignored.
     * @param table a collection of tables.
     * @return a collection of blocked tables.
     */
    @Override
    public Collection<Table> block(Collection<Table> table) {
        return table;
    }

    /**
     * Default iterator operation.
     * @param tables input table collection.
     */
    @Override
    public void iterator(Collection<Table> tables, IteratorStream<Tuple> iteratorStream) {
        Table table = tables.iterator().next();
        for (int i = 0; i < table.size(); i ++) {
            iteratorStream.put(table.get(i));
        }
    }

}