/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means “Clean” in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * MemoryTable represents a table which resides in memory.
 */
public class MemoryTable extends Table {
    private List<Tuple> tuples;

    private MemoryTable(List<Tuple> tuples) {
        super(tuples.get(0).getSchema());
        this.tuples = tuples;
    }

    public static MemoryTable of(List<Tuple> tuples) {
        Preconditions.checkArgument(tuples != null && tuples.size() > 0);
        return new MemoryTable(tuples);
    }

    //<editor-fold desc="Table override methods">
    @Override
    public void recycle() {
        tuples.clear();
        tuples = null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int size() {
        return tuples.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Tuple get(int i) {
        Preconditions.checkArgument(i >= 0 && i < size());
        return tuples.get(i);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Table project(List<Column> columns) {
        Column[] allColumns = schema.getColumns();
        List<Column> toRemove = Lists.newArrayList();

        for (Column column : allColumns) {
            for (Column column_ : columns) {
                if (!column.equals(column_)) {
                    toRemove.add(column);
                }
            }
        }

        Schema newSchema = new Schema(schema);

        for (Column column : toRemove) {
            newSchema.remove(column);
        }

        for (Tuple tuple : tuples) {
            tuple.project(newSchema);
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Table orderBy(List<Column> columns) {
        Collections.sort(tuples, TupleComparator.of(columns));
        return this;
    }

    /**
     * {@inheritDoc}
     */
    // TODO: currently only equalization is supported.
    @Override
    public Table filter(List<SimpleExpression> expressions) {
        List<Tuple> filtered = Lists.newArrayList();
        for (SimpleExpression expression : expressions) {
            Column column = expression.getLeft();
            String value = expression.getValue();
            for (Tuple tuple : tuples) {
                String v = tuple.get(column).toString();
                if (!v.equals(value)) {
                    filtered.add(tuple);
                }
            }
        }

        for (Tuple tuple : filtered) {
            tuples.remove(tuple);
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<Table> groupOn(List<Column> columns) {
        Preconditions.checkNotNull(columns);
        Schema schema = getSchema();

        for (Column column : columns) {
            if (!schema.hasColumn(column)) {
                throw new IllegalArgumentException("Column " + column + "does not exist.");
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
                groups.add(new MemoryTable(curList));
                curList = Lists.newArrayList();
                curList.add(tuple);
            }

            lastTuple = tuple;
        }

        groups.add(new MemoryTable(curList));
        return groups;
    }
}
