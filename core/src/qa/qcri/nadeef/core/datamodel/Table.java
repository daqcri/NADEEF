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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * A Table represents a collection of <code>Tuples</code>.
 */
public abstract class Table {
    protected Schema schema;

    /**
     * Constructor.
     * @param schema schema.
     */
    public Table(Schema schema) {
        this.schema = schema;
    }

    /**
     * Gets the Tuple schema.
     * @return Tuple schema.
     */
    public Schema getSchema() {
        return schema;
    }

    /**
     * Creates a <code>Table</code> from a collection of tuples.
     * @param tuples a collection of tuples.
     * @return <code>Table</code> instance.
     */
    protected abstract Table newTable(Collection<Tuple> tuples);

    /**
     * Gets the size of the collection.
     * @return size of the collection.
     */
    public abstract int size();

    /**
     * Gets the <code>Tuple</code> at the index.
     * @param i index.
     * @return Tuple.
     */
    public abstract Tuple get(int i);

    //<editor-fold desc="Default Table behavior">
    // TODO: implements default behaviors
    public abstract Table project(String columnName);
    public abstract Table project(Column column);
    public abstract Table project(Collection<Column> columns);
    public abstract Table orderBy(String columnName);
    public abstract Table orderBy(Column column);
    public abstract Table orderBy(Collection<Column> columns);
    public abstract Table filter(SimpleExpression expression);
    public abstract Table filter(List<SimpleExpression> expressions);

    /**
     * Clean up the resources for this <code>Table</code>. After recycling the table instance
     * should not be used any more.
     */
    public void recycle() {};

    /**
     * Partition the Table into multiple tuple Table based on a list of columns.
     * @param columns Partitions based on a list of column.
     * @return A collection of tuple collections.
     */
    public Collection<Table> groupOn(Collection<Column> columns) {
        Preconditions.checkNotNull(columns);
        Schema schema = getSchema();

        for (Column column : columns) {
            if (!schema.hasColumn(column)) {
                throw
                    new IllegalArgumentException(
                        "Column " + column + "does not exist."
                    );
            }
        }

        List<Table> groups = Lists.newArrayList();
        orderBy(columns);
        if (size() < 2) {
            groups.add(this);
            return groups;
        }

        Tuple lastTuple = get(0);
        List<Tuple> curList = Lists.newArrayList();
        curList.add(lastTuple);

        boolean isSameGroup = true;
        for (int i = 1; i < size(); i ++) {
            isSameGroup = true;
            Tuple tuple = get(i);
            for (Column column : columns) {
                Object lvalue = lastTuple.get(column);
                Object rvalue = tuple.get(column);
                if (!lvalue.equals(rvalue)) {
                    isSameGroup = false;
                    break;
                }

            }

            if (isSameGroup) {
                curList.add(tuple);
            } else {
                groups.add(newTable(curList));
                curList = Lists.newArrayList();
                curList.add(tuple);
            }

            lastTuple = tuple;
        }

        groups.add(newTable(curList));
        return groups;
    }

    /**
     * Partition the table into multiple table based
     * on the column.
     * @param columnName Partition based on column attribute name.
     * @return A collection of tuple collections.
     */
    public Collection<Table> groupOn(String columnName) {
        return groupOn(new Column(getSchema().getTableName(), columnName));
    }

    /**
     * Partition the table into multiple table based
     * on the column.
     * @param column Paritition based on column.
     * @return A collection of tuple collections.
     */
    public Collection<Table> groupOn(Column column) {
        Schema schema = getSchema();

        if (!schema.hasColumn(column)) {
            throw
                new IllegalArgumentException(
                    "Column " + column + "does not exist."
                );
        }

        List<Table> groups = Lists.newArrayList();
        if (size() < 2) {
            groups.add(this);
            return groups;
        }

        orderBy(column);

        Tuple lastTuple = get(0);
        List<Tuple> curList = Lists.newArrayList();
        curList.add(lastTuple);

        for (int i = 1; i < size(); i ++) {
            Tuple tuple = get(i);
            Object lvalue = lastTuple.get(column);
            Object rvalue = tuple.get(column);
            if (lvalue.equals(rvalue)) {
                curList.add(tuple);
            } else {
                groups.add(newTable(curList));
                curList = Lists.newArrayList();
                curList.add(tuple);
            }
            lastTuple = tuple;
        }

        groups.add(newTable(curList));
        return groups;
    }
    //</editor-fold desc="Default Table behavior">
}
