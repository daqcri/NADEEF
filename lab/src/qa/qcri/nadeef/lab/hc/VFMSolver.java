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

package qa.qcri.nadeef.lab.hc;

import com.google.common.collect.*;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.Operation;

import java.util.HashSet;
import java.util.List;

/**
 * Value Frequency Solver. It is a solver to deal with EQ / NEQ only
 * {@link qa.qcri.nadeef.core.datamodel.Fix}. The implementation is by counting the
 * frequency of values and set the new value with the highest count.
 */
public class VFMSolver extends SolverBase {

    @Override
    public List<Fix> solve(HashSet<Fix> repairContext, HashSet<Cell> changed) {
        Multiset<Object> values = HashMultiset.create();
        HashSet<Cell> cells = Sets.newHashSet();

        // executing VFM
        // we start with counting both the EQ and NEQ.
        // TODO: we don't know how to deal with NEQ.
        // When there are only NEQ, we need to assign an variable
        // class.
        for (Fix fix : repairContext) {
            if (fix.isRightConstant()) {
                values.add(fix.getRightValue());
                cells.add(fix.getLeft());
            } else {
                cells.add(fix.getLeft());
                values.add(fix.getLeft().getValue());
                cells.add(fix.getRight());
                values.add(fix.getRight().getValue());
            }
        }

        // count all the existing cells and pick the highest count first.
        for (Cell cell : cells) {
            Object value = cell.getValue();
            values.add(value);
        }

        Multiset<Object> sortedMultiSet = Multisets.copyHighestCountFirst(values);
        Object highestFrequencyValue = sortedMultiSet.iterator().next();
        List<Fix> result = Lists.newArrayList();
        Fix.Builder builder = new Fix.Builder();
        for (Cell cell : cells) {
            if (cell.getValue().equals(highestFrequencyValue))
                continue;
            result.add(
                builder
                    .left(cell)
                    .right(highestFrequencyValue)
                    .op(Operation.EQ)
                    .build()
            );
        }
        return result;
    }
}
