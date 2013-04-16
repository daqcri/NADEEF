/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.CleanPlan;
import sun.reflect.generics.reflectiveObjects.TypeVariableImpl;

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
        // loop all the type signature to find the actual input type.
        for (Type type : types) {
            if (type instanceof TypeVariableImpl) {
                continue;
            }

            if (type instanceof ParameterizedType) {
                return ((ParameterizedType)type).getRawType();
            } else {
                return type;
            }
        }
        throw new IllegalStateException("Input type is not identified.");
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