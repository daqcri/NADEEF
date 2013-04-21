/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.DBConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.List;

/**
 * Export violations into the target place.
 */
public class ViolationExport extends Operator<Collection<Violation>, Boolean> {

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
     */
    @Override
    public Boolean execute(Collection<Violation> violations)
            throws
                ClassNotFoundException,
                SQLException,
                InstantiationException,
                IllegalAccessException {
        Connection conn = DBConnectionFactory.createNadeefConnection();
        Statement stat = conn.createStatement();
        for (Violation violation : violations) {
            List<ViolationRow> rows = Lists.newArrayList(violation.getRowCollection());
            for (ViolationRow row : rows) {
                String sql = getSQLInsert(violation.getRuleId(), row);
                stat.addBatch(sql);
            }
        }
        stat.executeBatch();
        conn.commit();
        stat.close();
        conn.close();
        return true;
    }

    /**
     * Converts a violation to SQL insert.
     * @param row violation.
     * @return sql statement.
     */
    private String getSQLInsert(String ruleId, ViolationRow row) {
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO");
        sqlBuilder.append(' ');
        sqlBuilder.append(
            NadeefConfiguration.getSchemaName() +
            "." + NadeefConfiguration.getViolationTableName()
        );
        sqlBuilder.append(" VALUES (default, ");
        sqlBuilder.append("'" + ruleId + "',");
        Column column = row.getColumn();
        sqlBuilder.append("'" + column.getTableName() + "',");
        sqlBuilder.append(row.getTupleId());
        sqlBuilder.append(",");
        sqlBuilder.append("'" + column.getAttributeName() + "',");
        sqlBuilder.append("'" + row.getAttributeValue().toString() + "')");
        return sqlBuilder.toString();
    }
}
