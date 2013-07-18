/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Stopwatch;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Table;
import qa.qcri.nadeef.tools.Tracer;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Query engine operator, which generates optimized queries based on given hints.
 */
public class ScopeOperator<E>
    extends Operator<Collection<Table>, Collection<Table>> {
    private Rule<E> rule;

    /**
     * Constructor.
     * @param rule
     */
    public ScopeOperator(Rule<E> rule) {
        this.rule = rule;
    }

    /**
     * Execute the operator.
     *
     * @param tables a collection of <code>Table</code> (tables).
     * @return output object.
     */
    @Override
    public Collection<Table> execute(Collection<Table> tables)
        throws Exception {
        Stopwatch stopwatch = new Stopwatch().start();
        // Here the horizontalScope needs to be called before vertical Scope since
        // it may needs the attributes which are going to be removed from verticals scope.
        Collection<Table> horizontalScopeResult = rule.horizontalScope(tables);
        setPercentage(0.5f);
        long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        long currentTime;
        Tracer.putStatsEntry(Tracer.StatType.HScopeTime, time);

        Collection<Table> verticalScopeResult =
            rule.verticalScope(horizontalScopeResult);

        currentTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        Tracer.putStatsEntry(Tracer.StatType.VScopeTime, currentTime - time);
        Tracer.putStatsEntry(Tracer.StatType.AfterScopeTuple, verticalScopeResult.size());

        Collection<Table> result = verticalScopeResult;

        // Currently we don't support co-group, so once a rule is working with two tables we
        // ignore the block function.
        if (!rule.supportTwoTables()) {
            result = rule.block(verticalScopeResult);
            Tracer.putStatsEntry(Tracer.StatType.Blocks, result.size());
        }

        stopwatch.stop();
        return result;
    }
}
