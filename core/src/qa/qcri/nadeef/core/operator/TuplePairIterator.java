/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.Tuple;
import qa.qcri.nadeef.core.datamodel.TupleCollection;
import qa.qcri.nadeef.core.datamodel.TuplePair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Iterator which generates pair tuples.
 */
public class TuplePairIterator extends Operator<TupleCollection, Collection<TuplePair>> {
    /**
     * Execute the operator.
     *
     * @param tuples input tuples.
     * @return output object.
     */
    @Override
    public Collection<TuplePair> execute(TupleCollection tuples) throws Exception {
        int size = tuples.size() * tuples.size() / 2;
        ArrayList<TuplePair> result = new ArrayList(size);
        for (int i = 0; i < tuples.size(); i ++) {
            for (int j = i + 1; j < tuples.size(); j ++) {
                TuplePair pair = new TuplePair(tuples.get(i), tuples.get(j));
                result.add(pair);
            }
        }
        return result;
    }
}
