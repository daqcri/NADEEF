/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.Tracer;

import java.sql.*;
import java.util.ArrayList;

/**
 * SQLIterator generates tuples for the rule. It also does the optimization
 * based on the input rule hints.
 */
public class SQLIterator extends Operator<Rule, Tuple[]> {
    /**
     * Execute the operator.
     *
     * @param rule input rule.
     * @return output object.
     */
    @Override
    public Tuple[] execute(Rule rule) {
        Connection conn = input.getConnection();
        Tuple[] result = null;
        try {
            if (conn == null || conn.isClosed()) {
                throw new IllegalArgumentException("SQLIterator input has no JDBC connection.");
            }

            String sql = getSQLStatement(conn, rule);
            Statement stat = conn.createStatement();
            ResultSet resultSet = stat.executeQuery(sql);
            conn.commit();

            ArrayList<Tuple> list = new ArrayList<>();
            int rowCount = 0;
            while(resultSet.next()) {
                rowCount ++;
                ResultSetMetaData metaData = resultSet.getMetaData();
                int count = metaData.getColumnCount();
                Cell[] cells = new Cell[count];
                Object[] values = new Object[count];
                for (int i = 0; i < count; i ++) {
                    String attributeName = metaData.getColumnName(i);
                    String tableName = metaData.getTableName(i);
                    cells[i] = new Cell(tableName, attributeName);
                    values[i] = resultSet.getObject(i);
                }

                list.add(
                    new Tuple(cells, values)
                );
            }

            if (rowCount > 0) {
                result = list.toArray(new Tuple[rowCount]);
            }

        } catch (SQLException ex) {
            Tracer tracer = Tracer.getTracer(SQLIterator.class);
            tracer.err(ex.getMessage());
        }
        return result;
    }

    /**
     * Generates the SQL statement from the input.
     * @param input Iterator input.
     * @return SQL statement.
     * TODO: adds a caching mechanism for SQL generation.
     */
    public String getSQLStatement(IteratorInput input) {
        Rule rule = input.getRule();
        RuleHintCollection hints = rule.getHints();
        Connection conn = input.getConnection();
        StringBuilder sqlBuilder = new StringBuilder();

        // We do query optimization based on the hints.
        // 1 - projection hint
        ProjectHint[] projects = (ProjectHint[])hints.getHint(RuleHintType.Project);
        if (projects != null) {
            StringBuilder selectBuilder = new StringBuilder("SELECT");
            StringBuilder fromBuilder = new StringBuilder("FROM ");

            for (ProjectHint hint : projects) {
                Cell[] projectAttributes = hint.getAttributes();
                for (Cell attribute : projectAttributes) {
                    selectBuilder.append(" ");
                    selectBuilder.append(attribute.getFullAttributeName());
                    selectBuilder.append(" AS \"");
                    selectBuilder.append(attribute.getFullTableName());
                    selectBuilder.append("\" ");

                    fromBuilder.append(" ");
                    fromBuilder.append(attribute.getFullTableName());
                    fromBuilder.append(" ");
                }
            }
        } else {
            // has no projection hint, we need to select all.
        }

        return sqlBuilder.toString();
    }
}
