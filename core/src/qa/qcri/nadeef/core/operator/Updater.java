/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.DBConnectionFactory;

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
    private CleanPlan cleanPlan;
    private HashSet<Cell> status;

    public Updater(CleanPlan cleanPlan) {
        this.cleanPlan = Preconditions.checkNotNull(cleanPlan);
        status = Sets.newHashSet();
    }

    /**
     * Execute the operator.
     *
     * @param fixes Fix collection.
     * @return output object.
     */
    @Override
    public Integer execute(Collection<Fix> fixes) throws Exception {
        Connection conn =
            DBConnectionFactory.createConnection(cleanPlan.getSourceDBConfig());
        Statement stat = conn.createStatement();
        int count = 0;
        try {
            String rightValue;
            for (Fix fix : fixes) {
                Cell cell = fix.getLeft();

                // when the result is ambiguous, we set it to NULL.
                if (status.contains(cell)) {
                    // TODO: what happens when you assign the same value twice
                    rightValue = "null";
                } else {
                    rightValue = fix.getRightValue();
                    status.add(cell);
                }
                Column column = cell.getColumn();
                String tableName = column.getTableName();
                stat.addBatch(
                    "UPDATE " + tableName +
                    " SET " + column.getAttributeName() + " = " + rightValue +
                    " WHERE tid = " + cell.getTupleId()
                );
                count ++;
            }
            stat.executeBatch();
            conn.commit();
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
