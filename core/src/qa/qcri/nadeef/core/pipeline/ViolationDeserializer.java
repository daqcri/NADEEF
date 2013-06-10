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

import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Violation;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.Violations;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;

/**
 * Deserializing violations from violation table.
 */
public class ViolationDeserializer extends Operator<Rule, Collection<Violation>> {

    /**
     * Execute the operator.
     *
     * @param rule input rule.
     * @return output violations from database.
     */
    @Override
    public Collection<Violation> execute(Rule rule) throws Exception {
        Connection conn = DBConnectionFactory.getNadeefConnection();
        Collection<Violation> result = null;
        try {
            Statement stat = conn.createStatement();
            ResultSet resultSet = stat.executeQuery(
                "SELECT * FROM " +
                    NadeefConfiguration.getViolationTableName() +
                    " WHERE RID = '" +
                    rule.getRuleName() +
                    "' ORDER BY vid"
            );

            result = Violations.fromQuery(resultSet);
            setPercentage(1f);
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
        return result;
    }
}
