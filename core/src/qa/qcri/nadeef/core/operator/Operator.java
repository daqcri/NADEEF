/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.CleanPlan;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Abstract class for an Operator.
 */
public abstract class Operator<TInput, TOutput> {
    protected CleanPlan cleanPlan;

    public Operator() {}

    public Operator(CleanPlan plan) {
        this.cleanPlan = plan;
    }

    /**
     * Gets the generic input class information at runtime.
     * @return inputType class.
     */
    public Type getInputType() {
        ParameterizedType parameterizedType =
                (ParameterizedType)getClass().getGenericSuperclass();

        Type[] types = parameterizedType.getActualTypeArguments();
        if (types[0] instanceof ParameterizedType) {
            return ((ParameterizedType)types[0]).getRawType();
        } else {
            return types[0];
        }
    }

    /**
     * Execute the operator.
     * @param input input object.
     * @return output object.
     */
    public abstract TOutput execute(TInput input) throws Exception;

    /**
     * Check whether the operator is executable.
     * @param input input object.
     * @return True when the operator can execute on the given input object.
     */
    public boolean canExecute(Object input) {
        Type inputClass = getInputType();
        return ((Class)inputClass).isInstance(input);
    }
}