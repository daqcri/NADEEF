/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Violation;
import qa.qcri.nadeef.tools.Tracer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Operator which executes the repair of a rule.
 */
public class ViolationRepair
    extends Operator<Collection<Violation>, Collection<Collection<Fix>>> {
    private Rule rule;

    public ViolationRepair(Rule rule) {
        this.rule = Preconditions.checkNotNull(rule);
    }

    /**
     * Execute the operator.
     *
     * @param violations input object.
     * @return output object.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Collection<Collection<Fix>> execute(Collection<Violation> violations)
        throws Exception {
        Stopwatch stopwatch = new Stopwatch().start();
        List<Collection<Fix>> result = Lists.newArrayList();
        int count = 0;
        for (Violation violation : violations) {
            Collection<Fix> fix = (Collection<Fix>)rule.repair(violation);
            result.add(fix);
            count ++;
            setPercentage(count / violations.size());
        }
        long elapseTime = stopwatch.elapsed(TimeUnit.MILLISECONDS) / violations.size();
        Tracer.putStatsEntry(Tracer.StatType.RepairCallTime, elapseTime);
        stopwatch.stop();
        return result;
    }
}
