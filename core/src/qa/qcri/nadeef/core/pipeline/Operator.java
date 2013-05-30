/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import sun.reflect.generics.reflectiveObjects.TypeVariableImpl;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * Abstract class for an Operator.
 */
public abstract class Operator<TInput, TOutput> {
    private double percentage;
    private TypeToken typeToken;
    protected CleanPlan cleanPlan;

    /**
     * Constructor.
     */
    public Operator() {
        this.typeToken = new TypeToken<TInput>(getClass()){};
    }

    /**
     * Constructor.
     * @param plan Clean plan.
     */
    public Operator(CleanPlan plan) {
        super();
        this.cleanPlan = plan;
    }

    /**
     * Gets the generic input class information at runtime.
     * @return inputType class.
     */
    // TODO: solve the reflection in a better way
    @SuppressWarnings("all")
    public Type getInputType() {
        Type genericSuperType = getClass().getGenericSuperclass();
        if (!(genericSuperType instanceof ParameterizedType)) {
            return typeToken.getRawType();
        }
        ParameterizedType parameterizedType = (ParameterizedType)genericSuperType;

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
     * Gets the current progress percentage of this operator [0 - 100].
     * @return percentage in integer.
     */
    double getPercentage() { return percentage; }

    /**
     * Reset is called before operator starts to function.
     */
    void reset() {
        this.percentage = 0.0f;
    }

    /**
     * Interrupt is called in situation when the operator needs to shutdown during running.
     */
    void interrupt() {}

    /**
     * Sets the percentage.
     */
    void setPercentage(double percentage) {
        Preconditions.checkArgument(percentage >= 0.0f && percentage <= 1.0f);
        this.percentage = percentage;
    }

    /**
     * Check whether the operator is executable.
     * @param input input object.
     * @return True when the operator can execute on the given input object.
     */
    boolean canExecute(Object input) {
        Type inputClass = getInputType();
        return ((Class)inputClass).isInstance(input);
    }
}