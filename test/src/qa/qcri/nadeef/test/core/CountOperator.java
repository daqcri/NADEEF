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

package qa.qcri.nadeef.test.core;

import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.pipeline.Operator;
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
    protected Integer execute(Integer count) throws Exception {
        tracer.info("This is " + count);
        count ++;
        return count;
    }
}
