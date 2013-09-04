/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.pipeline;

import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.util.sql.DBConnectionFactory;
import qa.qcri.nadeef.core.util.Fixes;
import qa.qcri.nadeef.tools.Tracer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;

/**
 * Export fix into the repair database.
 */
class FixExport extends Operator<Collection<Collection<Fix>>, Integer> {
    private static Tracer tracer = Tracer.getTracer(FixExport.class);
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
     */
    // TODO: this is not out-of-process safe.
    @Override
    public synchronized Integer execute(Collection<Collection<Fix>> fixCollection)
        throws SQLException {
        Connection conn = null;
        Statement stat = null;
        int count = 0;
        try {
            conn = DBConnectionFactory.getNadeefConnection();
            stat = conn.createStatement();
            int id = Fixes.generateFixId();
            for (Collection<Fix> fixes : fixCollection) {
                for (Fix fix : fixes) {
                    String sql = getSQLInsert(id, fix);
                    stat.addBatch(sql);
                    count ++;
                }
                id ++;
            }
            setPercentage(0.5f);
            stat.executeBatch();
            conn.commit();
        } catch (Exception ex) {
            tracer.err("Exporting Fixes failed", ex);
        } finally {
            Tracer.putStatsEntry(Tracer.StatType.FixExport, count);
            if (stat != null) {
                stat.close();
            }

            if (conn != null) {
                conn.close();
            }
        }

        return count;
    }

    /**
     * Converts a violation to SQL insert.
     */
    private String getSQLInsert(int id, Fix fix) {
        int vid = fix.getVid();
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
        sqlBuilder
            .append(NadeefConfiguration.getRepairTableName())
            .append(" VALUES (")
            .append(id)
            .append(',')
            .append(vid)
            .append(',');

        Cell cell = fix.getLeft();
        sqlBuilder.append(cell.getTupleId());
        sqlBuilder.append(',');
        sqlBuilder.append("'").append(cell.getColumn().getTableName()).append("',");
        sqlBuilder.append("'").append(cell.getColumn().getColumnName()).append("',");
        sqlBuilder.append("'").append(cell.getValue().toString()).append("',");

        sqlBuilder.append(fix.getOperation().getValue());
        sqlBuilder.append(',');
        if (!fix.isConstantAssign()) {
            cell = fix.getRight();
            sqlBuilder.append(cell.getTupleId());
            sqlBuilder.append(',');
            sqlBuilder.append("'").append(cell.getColumn().getTableName()).append("',");
            sqlBuilder.append("'").append(cell.getColumn().getColumnName()).append("',");
            sqlBuilder.append("'").append(cell.getValue().toString()).append("')");
        } else {
            sqlBuilder.append("null, null, null,'" + fix.getRightValue() + "')");
        }

        return sqlBuilder.toString();
    }
}
