/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.util.Tracer;

/**
 * A counter operator.
 */
public class CountOperator extends Operator {

    private static Tracer tracer = Tracer.getInstance();

    /**
     * Execute the operator.
     *
     * @param inputKey
     * @return
     */
    @Override
    public Object execute(Object inputKey) {
        Integer count = (Integer)inputKey;
        tracer.info("This is " + count);
        count ++;
        return count;
    }

    /**
     * Check whether the operator is executable.
     *
     * @param inputKey
     * @return
     */
    @Override
    public boolean canExecute(Object inputKey) {
        return true;
    }
}
