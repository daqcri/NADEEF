/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import org.apache.commons.lang3.tuple.Pair;
import qa.qcri.nadeef.core.datamodel.Tuple;

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
        return null;
    }
}
