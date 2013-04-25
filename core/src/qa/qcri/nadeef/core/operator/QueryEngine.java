/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.TupleCollection;

import java.util.Collection;

/**
 * Query engine operator, which generates optimized queries based on given hints.
 */
public class QueryEngine<TRule extends Rule, TOutput>
    extends Operator<Collection<TupleCollection>, TOutput> {
    private TRule rule;

    /**
     * Constructor.
     * @param rule
     */
    public QueryEngine(TRule rule) {
        this.rule = rule;
    }

    /**
     * Execute the operator.
     *
     * @param tuples a collection of <code>TupleCollection</code> (tables).
     * @return output object.
     */
    @Override
    public TOutput execute(Collection<TupleCollection> tuples) throws Exception {
        Collection<TupleCollection> result = rule.scope(tuples);
        return (TOutput)rule.group(result);
    }
}
