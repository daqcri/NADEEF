/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.TupleCollection;
import qa.qcri.nadeef.core.datamodel.TupleCollectionPair;
import qa.qcri.nadeef.core.datamodel.TuplePair;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Iterator which generates pair tuples from pair <code>TupleCollection</code>.
 */
public class TupleCollectionPairIterator
        extends Operator<TupleCollectionPair, Collection<TuplePair>> {
    /**
     * Execute the operator.
     *
     * @param tupleCollectionPair input object.
     * @return output object.
     */
    @Override
    public Collection<TuplePair> execute(TupleCollectionPair tupleCollectionPair)
            throws Exception {
        TupleCollection left = tupleCollectionPair.getLeft();
        TupleCollection right = tupleCollectionPair.getRight();

        int size = left.size() * right.size();
        ArrayList<TuplePair> result = new ArrayList(size);

        for (int i = 0; i < left.size(); i ++) {
            for (int j = 0; j < right.size(); j ++) {
                TuplePair pair = new TuplePair(left.get(i), right.get(i));
                result.add(pair);
            }
        }
        return result;
    }
}
