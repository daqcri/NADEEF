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
 * Export fix in the repair database.
 */
public class FixExport extends Operator<Collection<List<Fix>>, Integer> {
    private static Tracer tracer = Tracer.getTracer(ViolationExport.class);

    /**
     * Constructor.
     * @param plan clean plan.
     */
    public FixExport(CleanPlan plan) {
        super(plan);
    }

    /**
     * Export the violation.
     *
     * @param fixCollection a collection of fixes.
     * @return whether the exporting is successful or not.
     * TODO: this is not out-of-process safe.
     */
    @Override
    public synchronized Integer execute(Collection<List<Fix>> fixCollection)
        throws
        ClassNotFoundException,
        SQLException,
        InstantiationException,
        IllegalAccessException {
        Connection conn = DBConnectionFactory.createNadeefConnection();
        Statement stat = conn.createStatement();
        Integer count = 0;
        int id = Fix.generateFixId();
        for (List<Fix> fixes : fixCollection) {
            for (Fix fix : fixes) {
                String sql = getSQLInsert(id, fix);
                stat.addBatch(sql);
                count ++;
            }
            id ++;
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
    private String getSQLInsert(int id, Fix fix) {
        int vid = fix.getVid();
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO");
        sqlBuilder.append(' ');
        sqlBuilder.append(
            NadeefConfiguration.getSchemaName() +
                "." + NadeefConfiguration.getRepairTableName()
        );
        sqlBuilder.append(" VALUES (");
        sqlBuilder.append(id);
        sqlBuilder.append(", '" + vid + "',");
        Cell cell = fix.getLeft();
        sqlBuilder.append(cell.getTupleId());
        sqlBuilder.append("'" + cell.getColumn().getTableName() + "',");
        sqlBuilder.append("'" + cell.getColumn().getAttributeName() + "',");
        sqlBuilder.append("'" + cell.getAttributeValue().toString() + "',");

        sqlBuilder.append(fix.getOperation().getValue());

        cell = fix.getRight();
        sqlBuilder.append(cell.getTupleId());
        sqlBuilder.append("'" + cell.getColumn().getTableName() + "',");
        sqlBuilder.append("'" + cell.getColumn().getAttributeName() + "',");
        sqlBuilder.append("'" + cell.getAttributeValue().toString() + "',");
        return sqlBuilder.toString();
    }
}
