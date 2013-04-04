/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

/**
 * Abstract class for an Operator.
 */
public abstract class Operator {
    /**
     * Execute the operator.
     * @param inputKey
     * @return
     */
    public abstract Object execute(Object inputKey);

    /**
     * Check whether the operator is executable.
     * @param inputKey
     * @return
     */
    public abstract boolean canExecute(Object inputKey);
}
