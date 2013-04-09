/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.test;

import qa.qcri.nadeef.core.operator.Operator;
import qa.qcri.nadeef.core.util.Tracer;

/**
 * A counter operator.
 */
public class CountOperator extends Operator<Integer, Integer> {

    private static Tracer tracer = Tracer.getTracer(CountOperator.class);

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
