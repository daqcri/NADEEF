/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.Tuple;
import qa.qcri.nadeef.core.datamodel.TupleCollection;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Iterator which generates tuple collections.
 */
public class TupleCollectionIterator
    extends Operator<Collection<Tuple>, TupleCollection> {

    /**
     * Execute the operator.
     *
     * @param tupleCollection input object.
     * @return output object.
     */
    @Override
    public TupleCollection execute(Collection<Tuple> tupleCollection) throws Exception {
        return new TupleCollection(tupleCollection);
    }
}
