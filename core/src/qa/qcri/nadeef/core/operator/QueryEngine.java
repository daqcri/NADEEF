/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.TupleCollection;

import java.util.Collection;
import java.util.List;

/**
 * Query engine operator, which generates optimized queries based on given hints.
 */
// TODO: remove this class and make a separate operator for block, iterator, and scope
public class QueryEngine<TDetect, TIteratorOutput>
    extends Operator<Collection<TupleCollection>, TIteratorOutput> {
    private Rule<TDetect, TIteratorOutput> rule;

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
    public TIteratorOutput execute(Collection<TupleCollection> tuples) throws Exception {
        Collection<TupleCollection> scopeResult = rule.scope(tuples);
        Collection<TupleCollection> blockResult = rule.block(scopeResult);
        List result = Lists.newArrayList();
        for (TupleCollection tupleCollection : blockResult) {
            TIteratorOutput iteratorResult = rule.iterator(tupleCollection);
            if (iteratorResult instanceof Collection) {
                result.addAll((Collection)iteratorResult);
            } else {
                result.add(iteratorResult);
            }
        }
        return (TIteratorOutput)result;
    }
}
