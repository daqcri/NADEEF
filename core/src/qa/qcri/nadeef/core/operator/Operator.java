/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import java.lang.reflect.Field;

/**
 * Abstract class for an Operator.
 */
public abstract class Operator<TInput, TOutput> {
    protected final Class<TInput> inputType = null;
    protected final Class<TOutput> outputType = null;

    /**
     * Gets the generic input class information at runtime.
     * @return
     */
    public Class getInputClass() {
        Class result = null;
        try {
            Field field = getClass().getField("inputType");
            result = field.getGenericType().getClass();
        } catch (NoSuchFieldException ex) {}
        return result;
    }

    /**
    * Gets the generic output class information at runtime.
    */
    public Class getOutputClass() {
        Class result = null;
        try {
            Field field = getClass().getField("inputType");
            result = field.getGenericType().getClass();
        } catch (NoSuchFieldException ex) {}
        return result;
    }

    /**
     * Execute the operator.
     * @param input input object.
     * @return output object.
     */
    public abstract TOutput execute(TInput input);

    /**
     * Check whether the operator is executable.
     * @param input input object.
     * @return True when the operator can execute on the given input object.
     */
    public boolean canExecute(Object input) {
        return getInputClass().isInstance(input);
    }
}