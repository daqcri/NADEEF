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

import com.google.common.collect.Maps;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.tools.CommonTools;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;

/**
 * Updater fixes the source data and exports it in the database.
 * It returns <code>True</code> when there is no Cell changed in
 * the pipeline. In this case the pipeline will stop.
 */
public class Updater extends Operator<Collection<Fix>, Integer> {
    private static Tracer tracer = Tracer.getTracer(Updater.class);
    private static HashMap<Cell, String> updateHistory = Maps.newHashMap();

    /**
     * Constructor.
     */
    public Updater() {
        updateHistory.clear();
    }

    /**
     * Apply the fixes from EQ and modify the original database.
     *
     * @param fixes Fix collection.
     * @return output object.
     */
    @Override
    public Integer execute(Collection<Fix> fixes) throws Exception {
        int count = 0;
        Connection conn = null;
        Statement stat = null;
        PreparedStatement auditInsertStat = null;
        String auditTableName = NadeefConfiguration.getAuditTableName();
        String rightValue;
        String oldValue;

        try {
            conn = DBConnectionFactory.getSourceConnection();
            stat = conn.createStatement();
            auditInsertStat =
                conn.prepareStatement(
                    "INSERT INTO " + auditTableName +
                    " VALUES (default, ?, ?, ?, ?, ?, ?, current_timestamp)");
            for (Fix fix : fixes) {
                Cell cell = fix.getLeft();
                oldValue = cell.getValue().toString();

                if (updateHistory.containsKey(cell)) {
                    String value = updateHistory.get(cell);
                    if (value.equals(fix.getRightValue())) {
                        continue;
                    }
                    // when a cell is set twice with different value,
                    // we set it to null for ambiguous value.
                    rightValue = "?";
                } else {
                    rightValue = fix.getRightValue();
                    updateHistory.put(cell, rightValue);
                }

                // check for numerical type.
                if (!CommonTools.isNumericalString(rightValue)) {
                    rightValue = '\'' + rightValue + '\'';
                }

                if (!CommonTools.isNumericalString(oldValue)) {
                    oldValue = '\'' + oldValue + '\'';
                }

                Column column = cell.getColumn();
                String tableName = column.getTableName();
                String updateSql =
                    "UPDATE " + tableName +
                    " SET " + column.getColumnName() + " = " + rightValue +
                    " WHERE tid = " + cell.getTupleId();
                tracer.verbose(updateSql);
                stat.addBatch(updateSql);
                auditInsertStat.setInt(1, fix.getVid());
                auditInsertStat.setInt(2, cell.getTupleId());
                auditInsertStat.setString(3, column.getTableName());
                auditInsertStat.setString(4, column.getColumnName());
                auditInsertStat.setString(5, oldValue);
                auditInsertStat.setString(6, rightValue);
                auditInsertStat.addBatch();
                if (count % 1024 == 0) {
                    auditInsertStat.executeBatch();
                }
                count ++;
                setPercentage(count / fixes.size());
            }
            stat.executeBatch();
            auditInsertStat.executeBatch();
            conn.commit();
            Tracer.putStatsEntry(Tracer.StatType.UpdatedCellNumber, count);
        } finally {
            if (conn != null) {
                conn.close();
            }

            if (stat != null) {
                stat.close();
            }
        }
        return Integer.valueOf(count);
    }
}
