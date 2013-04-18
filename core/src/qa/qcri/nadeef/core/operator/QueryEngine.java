/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.TupleCollection;

/**
 * Query engine operator, which generates optimized queries based on given hints.
 */
public class QueryEngine extends Operator<TupleCollection, TupleCollection> {
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
    public TupleCollection execute(TupleCollection tuples) throws Exception {
        rule.filter(tuples);
        rule.group(tuples);
        return tuples;
    }
}
