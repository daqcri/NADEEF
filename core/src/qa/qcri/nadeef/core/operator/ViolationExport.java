/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.Violation;
import qa.qcri.nadeef.core.util.DBConnectionFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Export violations into the target place.
 */
public class ViolationExport extends Operator<Violation[], Boolean> {

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
    public Boolean execute(Violation[] violations)
            throws
                ClassNotFoundException,
                SQLException,
                InstantiationException,
                IllegalAccessException {
        Connection conn = DBConnectionFactory.createTargetConnection(cleanPlan);
        Statement stat = conn.createStatement();
        for (Violation violation : violations) {
            String sql = getSQLInsert(violation);
            stat.addBatch(sql);
        }
        stat.executeBatch();
        stat.close();
        conn.close();
        return true;
    }

    /**
     * Converts a violation to SQL insert.
     * @param violation violation.
     * @return sql statement.
     */
    private String getSQLInsert(Violation violation) {
        NadeefConfiguration config = NadeefConfiguration.getInstance();
        StringBuilder sqlBuilder = new StringBuilder("INSERT INTO");
        sqlBuilder.append(' ');
        sqlBuilder.append(
            config.getNadeefSchemaName() + "." + config.getNadeefViolationTableName()
        );
        sqlBuilder.append(" VALUES (");
        sqlBuilder.append("'" + violation.getRuleId() + "',");
        Cell cell = violation.getCell();
        sqlBuilder.append("'" + cell.getTableName() + "',");
        sqlBuilder.append(0);
        sqlBuilder.append(",");
        sqlBuilder.append("'" + violation.getAttributeValue().toString() + "')");
        return sqlBuilder.toString();
    }
}
