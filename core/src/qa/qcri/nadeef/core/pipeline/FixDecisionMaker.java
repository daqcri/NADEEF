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

import qa.qcri.nadeef.core.datamodel.Fix;

import java.util.Collection;

/**
 * FixDecisionMaker provides algorithm which selects the right candidate fix based on a
 * collection of @see Fix.
 */
public abstract class FixDecisionMaker extends Operator<Collection<Fix>, Collection<Fix>> {
    /**
     * Constructor.
     */
    public FixDecisionMaker() {}

    /**
     * Decides which fixes are right given a collection of candidate fixes.
     *
     * @param fixes candidate fixes.
     * @return a collection of right @see Fix.
     */
    public abstract Collection<Fix> decide(Collection<Fix> fixes);

    /**
     * Execute the operator.
     *
     * @param fixes input object.
     * @return output object.
     */
    @Override
    protected final Collection<Fix> execute(Collection<Fix> fixes) throws Exception {
        return decide(fixes);
    }
}
