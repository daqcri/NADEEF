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
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.Fixes;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;

/**
 * Imports the fix data from database.
 */
class FixImport extends Operator<Rule, Collection<Fix>> {
    @Override
    public Collection<Fix> execute(Rule rule) throws Exception {
        Connection conn = DBConnectionFactory.getNadeefConnection();
        Collection<Fix> result = null;
        try {
            Statement stat = conn.createStatement();
            ResultSet resultSet =
                stat.executeQuery(
                    "select r.* from " +
                    NadeefConfiguration.getRepairTableName() +
                    " r where r.vid in (select vid from " +
                    NadeefConfiguration.getViolationTableName() +
                    " where rid = '" +
                    rule.getRuleName() +
                    "') order by r.vid"
                );


            result = Fixes.fromQuery(resultSet);
            Tracer.putStatsEntry(Tracer.StatType.FixDeserialize, result.size());
        } finally {
            if (conn != null) {
                conn.close();
            }
        }
        return result;
    }
}
