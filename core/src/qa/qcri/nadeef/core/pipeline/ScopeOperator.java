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

import com.google.common.base.Stopwatch;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Table;
import qa.qcri.nadeef.tools.Tracer;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Query engine operator, which generates optimized queries based on given hints.
 */
public class ScopeOperator<E>
    extends Operator<Collection<Table>, Collection<Table>> {
    private Rule<E> rule;

    /**
     * Constructor.
     * @param rule
     */
    public ScopeOperator(Rule<E> rule) {
        this.rule = rule;
    }

    /**
     * Execute the operator.
     *
     * @param tables a collection of <code>Table</code> (tables).
     * @return output object.
     */
    @Override
    public Collection<Table> execute(Collection<Table> tables)
        throws Exception {
        Stopwatch stopwatch = new Stopwatch().start();
        // Here the horizontalScope needs to be called before vertical Scope since
        // it may needs the attributes which are going to be removed from verticals scope.
        Collection<Table> horizontalScopeResult = rule.horizontalScope(tables);
        setPercentage(0.5f);
        long time = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        long currentTime;
        Tracer.putStatsEntry(Tracer.StatType.HScopeTime, time);

        Collection<Table> verticalScopeResult =
            rule.verticalScope(horizontalScopeResult);

        currentTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        Tracer.putStatsEntry(Tracer.StatType.VScopeTime, currentTime - time);
        Tracer.putStatsEntry(Tracer.StatType.AfterScopeTuple, verticalScopeResult.size());

        Collection<Table> result = verticalScopeResult;

        // Currently we don't support co-group, so once a rule is working with two tables we
        // ignore the block function.
        if (!rule.supportTwoTables()) {
            result = rule.block(verticalScopeResult);
            Tracer.putStatsEntry(Tracer.StatType.Blocks, result.size());
        }

        stopwatch.stop();
        return result;
    }
}
