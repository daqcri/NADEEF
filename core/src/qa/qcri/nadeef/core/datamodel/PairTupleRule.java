/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;

/**
 * PairTupleRule represents a rule which deals with pair of tuples.
 */
public abstract class PairTupleRule extends Rule<TuplePair> {
    //<editor-fold desc="Constructor">
    public PairTupleRule() {
        super();
    }

    public PairTupleRule(String id, List<String> tableNames) {
        super(id, tableNames);
    }
    //</editor-fold>

    /**
     * Detect rule with pair tuple.
     *
     * @param pair input tuple pair.
     * @return Violation set.
     */
    public abstract Collection<Violation> detect(TuplePair pair);

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
     * Default iterator operation.
     *
     * @param tupleCollections input tuple
     * @return a generator of tuple collection.
     */
    @Override
    public boolean iterator(TupleCollection tupleCollections, IteratorStream iteratorStream) {
        List<TupleCollection> collectionList = Lists.newArrayList(tupleCollections);

        if (collectionList.size() == 1) {
            TupleCollection tuples = collectionList.get(0);
            for (int i = 0; i < tuples.size(); i ++) {
                for (int j = i + 1; j < tuples.size(); j ++) {
                    TuplePair pair = new TuplePair(tuples.get(i), tuples.get(j));
                    iteratorStream.put(pair);
                }
            }
        } else {
            TupleCollection left = collectionList.get(0);
            TupleCollection right = collectionList.get(0);
            for (int i = 0; i < left.size(); i ++) {
                for (int j = i + 1; j < right.size(); j ++) {
                    TuplePair pair = new TuplePair(left.get(i), right.get(j));
                    iteratorStream.put(pair);
                }
            }

        }
        return true;
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
}
