/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.Tuple;
import qa.qcri.nadeef.core.datamodel.TuplePair;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Iterator which generates pair tuples.
 */
public class TuplePairIterator extends Operator<Collection<Tuple>, Collection<TuplePair>> {
    /**
     * Execute the operator.
     *
     * @param tupleCollection input tuples.
     * @return output object.
     */
    @Override
    public Collection<TuplePair> execute(Collection<Tuple> tupleCollection) throws Exception {
        int size = tupleCollection.size() * tupleCollection.size() / 2;
        List<Tuple> tuples = (List)tupleCollection;
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
