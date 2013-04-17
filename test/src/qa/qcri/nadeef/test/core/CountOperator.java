/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.core;

import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.operator.Operator;
import qa.qcri.nadeef.tools.Tracer;

/**
 * A counter operator.
 */
public class CountOperator extends Operator<Integer, Integer> {

    private static Tracer tracer = Tracer.getTracer(CountOperator.class);

    public CountOperator(CleanPlan plan) {
        super(plan);
    }

    /**
     * Execute the operator.
     */
    @Override
    public Integer execute(Integer count) {
        tracer.info("This is " + count);
        count ++;
        return count;
    }
}