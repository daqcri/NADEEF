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

package qa.qcri.nadeef.test.core;

import qa.qcri.nadeef.core.pipeline.ExecutionContext;
import qa.qcri.nadeef.core.pipeline.Operator;
import qa.qcri.nadeef.tools.Tracer;

/**
 * A counter operator.
 */
public class CountOperator extends Operator<Integer, Integer> {

    private static Tracer tracer = Tracer.getTracer(CountOperator.class);

    public CountOperator(ExecutionContext context) {
        super(context);
    }


    /**
     * Execute the operator.
     */
    @Override
    protected Integer execute(Integer count) throws Exception {
        tracer.info("This is " + count);
        count ++;
        return count;
    }
}
