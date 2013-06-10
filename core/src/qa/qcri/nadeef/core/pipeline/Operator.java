/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;
import qa.qcri.nadeef.core.datamodel.CleanPlan;

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
    protected abstract TOutput execute(TInput input) throws Exception;

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