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
public class QueryEngine extends Operator<TupleCollection, Collection<TupleCollection>> {
    private Rule rule;
    /**
     * Constructor.
     * @param rule
     */
    public QueryEngine(Rule rule) {
        this.rule = rule;
    }

    /**
     * Execute the operator.
     *
     * @param tuples a collection of <code>TupleCollection</code> (tables).
     * @return output object.
     */
    @Override
    public Collection<TupleCollection> execute(TupleCollection tuples) throws Exception {
        TupleCollection result = rule.filter(tuples);
        Collection<TupleCollection> resultCollection = rule.group(result);
        return resultCollection;
    }
}
