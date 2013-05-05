/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.Tools;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashSet;

/**
 * Updater fixes the source data and exports it in the database.
 * It returns <code>True</code> when there is no Cell changed in
 * the pipeline. In this case the pipeline will stop.
 */
public class Updater extends Operator<Collection<Fix>, Integer> {
    private static Tracer tracer = Tracer.getTracer(Updater.class);
    private CleanPlan cleanPlan;
    private HashSet<Cell> status;

    /**
     * Constructor.
     * @param cleanPlan input clean plan.
     */
    public Updater(CleanPlan cleanPlan) {
        this.cleanPlan = Preconditions.checkNotNull(cleanPlan);
        status = Sets.newHashSet();
    }

    /**
     * Apply the fixes from EQ and modify the original database.
     * TODO: store the audit message.
     *
     * @param fixes Fix collection.
     * @return output object.
     */
    @Override
    public Integer execute(Collection<Fix> fixes) throws Exception {
        int count = 0;
        Connection conn = null;
        Statement stat = null;
        String auditTableName = NadeefConfiguration.getAuditTableName();
        String rightValue;
        String oldValue;

        try {
            conn =
                DBConnectionFactory.createConnection(cleanPlan.getSourceDBConfig());
            stat = conn.createStatement();
            for (Fix fix : fixes) {
                Cell cell = fix.getLeft();
                oldValue = cell.getAttributeValue().toString();
                // when the result is ambiguous, we set it to NULL.
                if (status.contains(cell)) {
                    // TODO: what happens when you assign the same value twice
                    rightValue = "null";
                } else {
                    rightValue = fix.getRightValue();
                    status.add(cell);
                }

                // check for numerical type.
                if (!Tools.isNumericalString(rightValue)) {
                    rightValue = '\'' + rightValue + '\'';
                }

                if (!Tools.isNumericalString(oldValue)) {
                    oldValue = '\'' + oldValue + '\'';
                }

                Column column = cell.getColumn();
                String tableName = column.getTableName();
                String updateSql =
                    "UPDATE " + tableName +
                        " SET " + column.getAttributeName() + " = " + rightValue +
                        " WHERE tid = " + cell.getTupleId();
                tracer.verbose(updateSql);
                stat.addBatch(updateSql);
                String insertSql =
                    "INSERT INTO " + auditTableName +
                    " VALUES (default, " +
                    fix.getVid() + "," +
                    cell.getTupleId() + ",\'" +
                    column.getTableName() + "\',\'" +
                    column.getAttributeName() + "\'," +
                    oldValue + "," +
                    rightValue + "," +
                    "current_timestamp)";
                tracer.verbose(insertSql);
                stat.addBatch(insertSql);
                count ++;
            }
            stat.executeBatch();
            conn.commit();
            tracer.info("In total there are " + count + " cells modified.");
        } finally {
            if (conn != null) {
                conn.close();
            }

            if (stat != null) {
                stat.close();
            }
        }
        return count;
    }
}
