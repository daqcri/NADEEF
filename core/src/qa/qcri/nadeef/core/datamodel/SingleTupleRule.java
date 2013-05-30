/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
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
     * @param tupleCollection input tuple collections.
     * @return filtered tuple collection.
     */
    @Override
    public Collection<TupleCollection> horizontalScope(
        Collection<TupleCollection> tupleCollection
    ) {
        return tupleCollection;
    }

    /**
     * Default scope operation.
     * @param tupleCollection input tuple collections.
     * @return filtered tuple collection.
     */
    @Override
    public Collection<TupleCollection> verticalScope(
        Collection<TupleCollection> tupleCollection
    ) {
        return tupleCollection;
    }

    /**
     * Default block operation.
     * @param tupleCollection a collection of tables.
     * @return a collection of blocked tables.
     */
    @Override
    public Collection<TupleCollection> block(Collection<TupleCollection> tupleCollection) {
        return tupleCollection;
    }

    /**
     * Default generator operation.
     * @param tupleCollection input tuple
     */
    @Override
    public void iterator(TupleCollection tupleCollection, IteratorStream iteratorStream) {
        for (int i = 0; i < tupleCollection.size(); i ++) {
            iteratorStream.put(tupleCollection.get(i));
        }
    }

}