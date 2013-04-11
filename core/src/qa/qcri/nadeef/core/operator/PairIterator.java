/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import qa.qcri.nadeef.core.datamodel.Tuple;

import java.util.ArrayList;

/**
 */
public class PairIterator extends Operator<Tuple[], Pair<Tuple, Tuple>[]> {
    /**
     * Execute the operator.
     *
     * @param tuples input tuples.
     * @return output object.
     */
    @Override
    public Pair<Tuple, Tuple>[] execute(Tuple[] tuples) throws Exception {
        int size = tuples.length * tuples.length / 2;
        ArrayList<Pair<Tuple, Tuple>> result = new ArrayList(size);
        for (int i = 0; i < tuples.length; i ++) {
            for (int j = i + 1; j < tuples.length; j ++) {
                Pair<Tuple, Tuple> pair = new ImmutablePair<Tuple, Tuple>(tuples[i], tuples[j]);
                result.add(pair);
            }
        }
        return result.toArray(new Pair[result.size()]);
    }
}
