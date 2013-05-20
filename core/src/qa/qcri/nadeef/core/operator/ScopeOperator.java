/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.base.Stopwatch;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.TupleCollection;
import qa.qcri.nadeef.tools.Tracer;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Query engine operator, which generates optimized queries based on given hints.
 */
public class ScopeOperator<E>
    extends Operator<Collection<TupleCollection>, Collection<TupleCollection>> {
    private Rule<E> rule;

    /**
     * Constructor.
     * @param rule
     */
    public ScopeOperator(Rule rule) {
        this.rule = rule;
    }

    /**
     * Execute the operator.
     *
     * @param tuples a collection of <code>TupleCollection</code> (tables).
     * @return output object.
     */
    @Override
    public Collection<TupleCollection> execute(Collection<TupleCollection> tuples) throws
        Exception {
        Stopwatch stopwatch = new Stopwatch().start();
        // Here the horizontalScope needs to be called before vertical Scope since
        // it may needs the attributes which are going to be removed from verticals scope.
        Collection<TupleCollection> horizontalScopeResult = rule.horizontalScope(tuples);
        long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        long currentTime;
        Tracer.addStatEntry(Tracer.StatType.HScopeTime, time);

        Collection<TupleCollection> verticalScopeResult =
            rule.verticalScope(horizontalScopeResult);

        currentTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        Tracer.addStatEntry(Tracer.StatType.VScopeTime, currentTime - time);
        Tracer.addStatEntry(Tracer.StatType.AfterScopeTuple, verticalScopeResult.size());
        Collection<TupleCollection> blockResult = rule.block(verticalScopeResult);
        Tracer.addStatEntry(Tracer.StatType.Blocks, blockResult.size());
        stopwatch.stop();
        return blockResult;
    }
}
