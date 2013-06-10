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
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.SQLTable;
import qa.qcri.nadeef.core.datamodel.Table;
import qa.qcri.nadeef.tools.DBConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * SourceDeserializer generates tuples for the rule. It also does the optimization
 * based on the input rule hints.
 */
public class SourceDeserializer extends Operator<Rule, Collection<Table>> {
    private DBConfig dbConfig;

    /**
     * Constructor.
     * @param plan Clean plan.
     */
    public SourceDeserializer(CleanPlan plan) {
        super(plan);
        dbConfig = plan.getSourceDBConfig();
    }

    /**
     * Execute the operator.
     *
     * @param rule input rule.
     * @return output object.
     */
    @Override
    @SuppressWarnings("unchecked")
    public Collection<Table> execute(Rule rule) {
        Preconditions.checkNotNull(rule);

        List<String> tableNames = (List<String>)rule.getTableNames();
        List<Table> collections = new ArrayList<Table>();
        if (tableNames.size() == 2) {
            collections.add(new SQLTable(tableNames.get(0), dbConfig));
            collections.add(new SQLTable(tableNames.get(1), dbConfig));
        } else {
            collections.add(new SQLTable(tableNames.get(0), dbConfig));
        }

        return collections;
    }
}
