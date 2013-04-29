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
 * Updator fixes the source data and exports it in the database.
 * It returns <code>True</code> when there is no Cell changed in
 * the pipeline. In this case the pipeline will stop.
 */
public class Updator extends Operator<Collection<Fix>, Boolean> {
    private CleanPlan cleanPlan;
    private HashSet<Column> status;

    public Updator(CleanPlan cleanPlan) {
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
    public Boolean execute(Collection<Fix> fixes) throws Exception {
        Connection conn =
            DBConnectionFactory.createConnection(cleanPlan.getSourceDBConfig());
        Statement stat = conn.createStatement();
        try {
            String rightValue;
            for (Fix fix : fixes) {
                Cell cell = fix.getLeft();
                Column column = cell.getColumn();

                // when the result is ambigious, we set it to NULL.
                if (status.contains(column)) {
                    // TODO: what happens when you assign the same value twice
                    rightValue = "null";
                } else {
                    rightValue = fix.getRightValue();
                }
                String tableName = column.getTableName();
                stat.addBatch(
                    "UPDATE " + tableName +
                    " SET " + column.getAttributeName() + " = " + rightValue +
                    " WHERE tid = " + cell.getTupleId()
                );
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
        return null;
    }
}
