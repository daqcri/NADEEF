/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Abstract Tuple Collection.
 */
public abstract class TupleCollection {
    protected Schema schema;

    /**
     * Constructor.
     * @param schema schema.
     */
    public TupleCollection(Schema schema) {
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
     * Creates a tuple collection from a collection of tuples.
     * @param tuples a collection of tuples.
     * @return <code>TupleCollection</code> instance.
     */
    protected abstract TupleCollection newTupleCollection(Collection<Tuple> tuples);

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

    //<editor-fold desc="Default TupleCollection behavior">
    // TODO: implements default behaviors
    public abstract TupleCollection project(String columnName);
    public abstract TupleCollection project(Column column);
    public abstract TupleCollection project(Collection<Column> columns);
    public abstract TupleCollection orderBy(String columnName);
    public abstract TupleCollection orderBy(Column column);
    public abstract TupleCollection orderBy(Collection<Column> columns);
    public abstract TupleCollection filter(SimpleExpression expression);
    public abstract TupleCollection filter(List<SimpleExpression> expressions);

    /**
     * Partition the tuple collection into multiple tuple collections based
     * on a list of columns.
     * @param columns Paritition based on a list of column.
     * @return A collection of tuple collections.
     */
    public Collection<TupleCollection> groupOn(Collection<Column> columns) {
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

        List<TupleCollection> groups = new ArrayList();
        orderBy(columns);
        if (size() < 2) {
            groups.add(this);
            return groups;
        }

        Tuple lastTuple = get(0);
        List<Tuple> curList = new ArrayList();
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
                groups.add(newTupleCollection(curList));
                curList = new ArrayList();
                curList.add(tuple);
            }

            lastTuple = tuple;
        }

        groups.add(newTupleCollection(curList));
        return groups;
    }

    /**
     * Partition the tuple collection into multiple tuple collections based
     * on the column.
     * @param column Paritition based on column.
     * @return A collection of tuple collections.
     */
    public Collection<TupleCollection> groupOn(Column column) {
        Schema schema = getSchema();

        if (!schema.hasColumn(column)) {
            throw
                new IllegalArgumentException(
                    "Column " + column + "does not exist."
                );
        }

        List<TupleCollection> groups = new ArrayList();
        if (size() < 2) {
            groups.add(this);
            return groups;
        }

        orderBy(column);

        Tuple lastTuple = get(0);
        List<Tuple> curList = new ArrayList();
        curList.add(lastTuple);

        for (int i = 1; i < size(); i ++) {
            Tuple tuple = get(i);
            Object lvalue = lastTuple.get(column);
            Object rvalue = tuple.get(column);
            if (lvalue.equals(rvalue)) {
                curList.add(tuple);
            } else {
                groups.add(newTupleCollection(curList));
                curList = new ArrayList();
                curList.add(tuple);
            }
            lastTuple = tuple;
        }

        groups.add(newTupleCollection(curList));
        return groups;
    }
    //</editor-fold desc="Default TupleCollection behavior">
}
