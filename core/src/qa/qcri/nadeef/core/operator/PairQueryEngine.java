/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.TupleCollectionPair;

/**
 * Query engine operator, which generates optimized queries based on given hints.
 */
public class PairQueryEngine extends Operator<TupleCollectionPair, TupleCollectionPair> {
    private Rule rule;
    /**
     * Constructor.
     * @param rule
     */
    public PairQueryEngine(Rule rule) {
        this.rule = rule;
    }

    /**
     * Execute the operator.
     *
     * @param tuples a collection of <code>TupleCollection</code> (tables).
     * @return output object.
     */
    @Override
    public TupleCollectionPair execute(TupleCollectionPair tuples) throws Exception {
        TupleCollectionPair result = (TupleCollectionPair)tuples;
        rule.filter(result.getLeft());
        rule.filter(result.getRight());
        return result;
    }
}
