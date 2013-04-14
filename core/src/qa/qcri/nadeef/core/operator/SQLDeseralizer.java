/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.DBConnectionFactory;
import qa.qcri.nadeef.core.util.Tracer;

import java.sql.*;
import java.util.*;

/**
 * SQLDeseralizer generates tuples for the rule. It also does the optimization
 * based on the input rule hints.
 */
public class SQLDeseralizer extends Operator<Rule, List<Tuple>> {

    /**
     * Constructor.
     * @param plan Clean plan.
     */
    public SQLDeseralizer(CleanPlan plan) {
        super(plan);
    }

    /**
     * Execute the operator.
     *
     * @param rule input rule.
     * @return output object.
     */
    @Override
    public List<Tuple> execute(Rule rule)
            throws
                ClassNotFoundException,
                SQLException,
                InstantiationException,
                IllegalAccessException {
        Connection conn = DBConnectionFactory.createSourceTableConnection(cleanPlan);
        List<Tuple> result = null;
        try {
            if (conn == null || conn.isClosed()) {
                throw new IllegalArgumentException("SQLDeseralizer has no JDBC connection.");
            }

            String sql = getSQLStatement(rule);
            Statement stat = conn.createStatement();
            ResultSet resultSet = stat.executeQuery(sql);
            conn.commit();

            ArrayList<Tuple> list = new ArrayList<>();
            while(resultSet.next()) {
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

                list.add(new Tuple(cells, values));
            }
        } catch (SQLException ex) {
            Tracer tracer = Tracer.getTracer(SQLDeseralizer.class);
            tracer.err(ex.getMessage());
        }
        return result;
    }

    /**
     * Generates the SQL statement from the input.
     * @param rule rule.
     * @return SQL statement.
     * TODO: adds a caching mechanism for SQL generation.
     */
    public String getSQLStatement(Rule rule) {
        RuleHintCollection hints = rule.getHints();
        StringBuilder sqlBuilder = new StringBuilder();
        StringBuilder selectBuilder = new StringBuilder(" SELECT ");
        StringBuilder fromBuilder = new StringBuilder(" FROM ");

        // We do query optimization based on the hints.
        // 1 - projection hint
        List<RuleHint> projects = hints.getHint(RuleHintType.Project);
        if (projects != null) {
            List<String> attrList = new ArrayList<>();

            for (RuleHint projectHint : projects) {
                ProjectHint hint = (ProjectHint)projectHint;
                List<Cell> projectAttributes = hint.getAttributes();
                for (int i = 0; i < projectAttributes.size(); i ++) {
                    attrList.add(projectAttributes.get(i).getFullAttributeName());
                }
            }

            // build up the select SQL.
            for (int i = 0; i < attrList.size(); i ++) {
                if (i != 0) {
                    selectBuilder.append(",");
                }
                selectBuilder.append(attrList.get(i));
            }
        } else {
            // has no projection hint, we need to select all.
            selectBuilder.append("*");
        }

        List<String> tableNames = rule.getTableNames();
        for (int i = 0; i < tableNames.size(); i ++) {
            if (i != 0) {
                fromBuilder.append(",");
            }
            fromBuilder.append(tableNames.get(i));
        }

        // 2 - group by hint

        sqlBuilder.append(selectBuilder);
        sqlBuilder.append(fromBuilder);
        return sqlBuilder.toString().trim();
    }
}
