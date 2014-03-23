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

package qa.qcri.nadeef.lab.holisticCleaning;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import gurobi.*;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.Operation;
import qa.qcri.nadeef.tools.Tracer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class GurobiSolver implements ISolver {
    private Tracer tracer = Tracer.getTracer(GurobiSolver.class);
    private final static double INF_SMALL = Integer.MIN_VALUE;

    public List<Fix> solve(HashSet<Fix> repairContext) {
    GRBEnv env = null;
    GRBModel model = null;
    try {
        env = new GRBEnv();
        model = new GRBModel(env);

        // encoding QP
        HashMap<Cell, GRBVar> cellIndex = Maps.newHashMap();
        HashMap<GRBVar, Cell> varIndex = Maps.newHashMap();

        int fixIndex = 0;
        // A linear scan to create all the variables and constraints.
        for (Fix fix : repairContext) {
            fixIndex ++;
            Cell cell = fix.getLeft();
            GRBVar var1;
            GRBVar var2 = null;
            if (cellIndex.containsKey(cell))
                var1 = cellIndex.get(cell);
            else {
                var1 =
                    model.addVar(
                        Integer.MIN_VALUE,
                        Integer.MAX_VALUE,
                        0.0,
                        GRB.CONTINUOUS,
                        cell.getColumn().getColumnName()
                    );
                cellIndex.put(cell, var1);
                varIndex.put(var1, cell);
            }

            if (!fix.isConstantAssign()) {
                cell = fix.getRight();
                if (cellIndex.containsKey(cell))
                    var2 = cellIndex.get(cell);
                else {
                    var2 =
                        model.addVar(
                            Integer.MIN_VALUE,
                            Integer.MAX_VALUE,
                            0.0,
                            GRB.CONTINUOUS,
                            cell.getColumn().getColumnName()
                        );
                    cellIndex.put(cell, var2);
                    varIndex.put(var2, cell);
                }
            }

            // update the model terms
            model.update();

            GRBLinExpr constraint = new GRBLinExpr();
            constraint.addTerm(1.0, var1);
            if (!fix.isConstantAssign()) {
                constraint.addTerm(-1.0, var2);
            } else {
                String rightValue = fix.getRightValue();
                boolean isInt = true;
                try {
                    int value = Integer.parseInt(rightValue);
                    constraint.addConstant(-1.0 * value);
                } catch (Exception ex) {
                    isInt = false;
                }

                if (!isInt) {
                    try {
                        double value = Double.parseDouble(rightValue);
                        constraint.addConstant(-1.0 * value);
                    } catch (Exception ex) {
                        tracer.err("Cannot convert right value into numerical value.", ex);
                    }
                }
            }

            switch (fix.getOperation()) {
                case EQ:
                    model.addConstr(constraint, GRB.EQUAL, 0.0, "c" + fixIndex);
                    break;
                case NEQ:
                    model.setObjective(constraint, GRB.MAXIMIZE);
                    break;
                case GT:
                    model.addConstr(constraint, GRB.GREATER_EQUAL, 1.0, "c" + fixIndex);
                    break;
                case GTE:
                    model.addConstr(constraint, GRB.GREATER_EQUAL, 0.0, "c" + fixIndex);
                    break;
                case LT:
                    model.addConstr(constraint, GRB.LESS_EQUAL, 0.0, "c" + fixIndex);
                    break;
                case LTE:
                    model.addConstr(constraint, GRB.LESS_EQUAL, -1.0, "c" + fixIndex);
                    break;
            }
        }

        // Apply Numerical Distance Minimality
        GRBQuadExpr numMinDistance = new GRBQuadExpr();
        for (Cell cell : cellIndex.keySet()) {
            GRBVar var = cellIndex.get(cell);
            double value = getValue(cell);
            numMinDistance.addTerm(1.0, var, var);
            numMinDistance.addConstant(value * value);
            numMinDistance.addTerm(-2.0 * value, var);
        }
        model.setObjective(numMinDistance, GRB.MINIMIZE);

        // Apply Cardinality Minimality
        int index = 0;
        for (Cell cell : cellIndex.keySet()) {
            GRBVar var = cellIndex.get(cell);
            double value = getValue(cell);
            GRBLinExpr constraint = new GRBLinExpr();
            constraint.addTerm(1.0, var);
            constraint.addConstant(-value);
            model.addConstr(constraint, GRB.LESS_EQUAL, INF_SMALL, "m" + index);
            index ++;
        }

        // Start optimizing
        model.update();

        // Sometimes the model itself has no solution, we need to
        // setup relaxation conditions to achieve maximum constraints.
        model.feasRelax(1, true, true, true);
        model.optimize();

        // export the output
        List<Fix> fixList = Lists.newArrayList();
        for (GRBVar var : varIndex.keySet()) {
            Cell cell = varIndex.get(var);
            double value = var.get(GRB.DoubleAttr.X);
            double oldValue = getValue(cell);

            // ignore the tiny difference
            if (Math.abs(oldValue - value) > 1e-7) {
                Fix fix = new Fix.Builder().left(cell).right(value).op(Operation.EQ).build();
                fixList.add(fix);
            }
        }
        return fixList;
    } catch (Exception ex) {
        tracer.err("Initializing GRB environment failed.", ex);
    }  finally {
        try {
            if (model != null)
                model.dispose();
            if (env != null)
                env.dispose();
        } catch (Exception ex) {
            tracer.err("Disposing Gurobi failed.", ex);
        }
    }
        return null;
    }

    private double getValue(Cell cell) {
        Object valueObject = cell.getValue();
        double value = 0;
        if (valueObject instanceof Integer) {
            value = (int)valueObject;
        } else {
            value = (double)valueObject;
        }
        return value;
    }
}
