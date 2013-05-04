/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.Violations;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

/**
 * Export violations into the target place.
 */
public class ViolationExport extends Operator<Collection<Violation>, Integer> {
    private static Tracer tracer = Tracer.getTracer(ViolationExport.class);

    /**
     * Constructor.
     * @param plan clean plan.
     */
    public ViolationExport(CleanPlan plan) {
        super(plan);
    }

    /**
     * Export the violation.
     *
     * @param violations violations.
     * @return whether the exporting is successful or not.
     * TODO: this is not out-of-process safe.
     */
    @Override
    public synchronized Integer execute(Collection<Violation> violations)
            throws
                ClassNotFoundException,
                SQLException,
                InstantiationException,
                IllegalAccessException {
        Connection conn = DBConnectionFactory.createNadeefConnection();
        Statement stat = conn.createStatement();
        // stat.execute("DELETE FROM " + NadeefConfiguration.getViolationTableName());
        Integer count = 0;
        int vid = Violations.generateViolationId();
        for (Violation violation : violations) {
            List<Cell> cells = Lists.newArrayList(violation.getCells());
            for (Cell cell : cells) {
                String sql = getSQLInsert(violation.getRuleId(), vid, cell);
                stat.addBatch(sql);
                count ++;
            }
            violation.setVid(vid);
            vid ++;
        }

        stat.executeBatch();
        conn.commit();
        stat.close();
        conn.close();
        tracer.info("exported " + count + " rows in Violation table.");
        return count;
    }

    /**
     * Converts a violation to SQL insert.
     */
    private String getSQLInsert(String ruleId, int vid, Cell cell) {
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO");
        sqlBuilder.append(' ');
        sqlBuilder.append(
            NadeefConfiguration.getSchemaName() +
            "." + NadeefConfiguration.getViolationTableName()
        );
        sqlBuilder.append(" VALUES (");
        sqlBuilder.append(vid);
        sqlBuilder.append(", '" + ruleId + "',");
        Column column = cell.getColumn();
        sqlBuilder.append("'" + column.getTableName() + "',");
        sqlBuilder.append(cell.getTupleId());
        sqlBuilder.append(",");
        sqlBuilder.append("'" + column.getAttributeName() + "',");
        sqlBuilder.append("'" + cell.getAttributeValue().toString() + "')");
        return sqlBuilder.toString();
    }
}
