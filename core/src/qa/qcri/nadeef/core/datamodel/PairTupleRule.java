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
     * Block operation.
     * @param table a collection of tables.
     * @return a collection of blocked tables.
     */
    @Override
    public Collection<Table> block(Collection<Table> table) {
        return table;
    }

    /**
     * Iterator operation.
     *
     * @param tables input tuple
     */
    @Override
    public void iterator(Collection<Table> tables, IteratorStream<TuplePair> iteratorStream) {
        List<Table> collectionList = Lists.newArrayList(tables);

        if (collectionList.size() == 1) {
            Table tuples = collectionList.get(0);
            for (int i = 0; i < tuples.size(); i ++) {
                for (int j = i + 1; j < tuples.size(); j ++) {
                    TuplePair pair = new TuplePair(tuples.get(i), tuples.get(j));
                    iteratorStream.put(pair);
                }
            }
        } else {
            Table left = collectionList.get(0);
            Table right = collectionList.get(1);
            for (int i = 0; i < left.size(); i ++) {
                for (int j = 0; j < right.size(); j ++) {
                    TuplePair pair = new TuplePair(left.get(i), right.get(j));
                    iteratorStream.put(pair);
                }
            }
        }
    }

    /**
     * Default scope operation.
     * @param table input tuple collections.
     * @return filtered tuple collection.
     */
    @Override
    public Collection<Table> horizontalScope(
        Collection<Table> table
    ) {
        return table;
    }

    /**
     * Default scope operation.
     * @param table input tuple collections.
     * @return filtered tuple collection.
     */
    @Override
    public Collection<Table> verticalScope(
        Collection<Table> table
    ) {
        return table;
    }
}
