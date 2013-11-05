/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.datamodel;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentMap;

/**
 * PairTupleRule represents a rule which deals with pair of tuples.
 *
 */
public abstract class PairTupleRule extends Rule<TuplePair> {
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
     * Incremental iterator interface.
     * @param blocks blocks.
     * @param newTuples new tuples comes in.
     * @param iteratorStream output stream.
     */
    public final void iterator(
        Collection<Table> blocks,
        ConcurrentMap<String, HashSet<Integer>> newTuples,
        IteratorStream<TuplePair> iteratorStream
    ) {
        // one block a time.
        Table block = blocks.iterator().next();

        String tableName = block.getSchema().getTableName();
        if (newTuples.containsKey(tableName)) {
            HashSet<Integer> newTuplesIDs = newTuples.get(tableName);

            // iterating all the tuples
            for (int i = 0; i < block.size(); i++) {
                Tuple tuple1 = block.get(i);
                if (newTuplesIDs.contains(tuple1.getTid())) {
                    for (int j = 0; j < block.size(); j++) {
                        if (j != i) {
                            Tuple tuple2 = block.get(j);
                            if (newTuplesIDs.contains(tuple2.getTid())) {
                                // Both are new tuples, check once
                                if (j > i) {
                                    iteratorStream.put(new TuplePair(tuple1, tuple2));
                                }
                            } else {
                                // Compare with old tuples
                                iteratorStream.put(new TuplePair(tuple1, tuple2));
                            }
                        }
                    }
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
    public Collection<Table> horizontalScope(Collection<Table> table) {
        return table;
    }

    /**
     * Default scope operation.
     * @param table input tuple collections.
     * @return filtered tuple collection.
     */
    @Override
    public Collection<Table> verticalScope(Collection<Table> table) {
        return table;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasOwnIterator() {
        boolean result = false;
        try {
            String declareClassName =
                getClass().getMethod(
                    "iterator",
                    new Class[] { Collection.class, IteratorStream.class }
                ).getDeclaringClass().getSimpleName();
            result = !declareClassName.equalsIgnoreCase("PairTupleRule");
        } catch (Exception ex) {}
        return result;
    }
}
