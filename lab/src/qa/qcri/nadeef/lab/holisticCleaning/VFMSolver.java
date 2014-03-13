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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Lists;
import com.google.common.collect.Multiset;
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.Operation;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

/**
 * Value Frequency Solver. It is a solver to deal with EQ / NEQ only
 * {@link qa.qcri.nadeef.core.datamodel.Fix}. The implementation is by counting the
 * frequency of values and set the new value with the highest count.
 */
public class VFMSolver implements ISolver {
    @Override
    public List<Fix> solve(HashSet<Fix> repairContext) {
        HashMap<Cell, Multiset<Object>> eqCount = new HashMap<>();
        HashMap<Cell, List<Object>> neqCount = new HashMap<>();

        // executing VFM
        // we start with counting both the EQ and NEQ.
        for (Fix fix : repairContext) {
            Cell cell = fix.getLeft();
            Object value;
            if (fix.isConstantAssign())
                value = fix.getRightValue();
            else
                value = fix.getRight().getValue();

            if (fix.getOperation() == Operation.EQ) {
                if (!eqCount.containsKey(cell)) {
                    Multiset<Object> multiset = HashMultiset.create();
                    multiset.add(value);
                    eqCount.put(cell, multiset);
                } else {
                    Multiset<Object> multiset = eqCount.get(cell);
                    multiset.add(value);
                }
            } else {
                if (!neqCount.containsKey(cell)) {
                    List<Object> neqList = Lists.newArrayList();
                    neqList.add(value);
                    neqCount.put(cell, neqList);
                } else {
                    List<Object> neqList = Lists.newArrayList();
                    neqList.add(value);
                }
            }
        }

        // Deduce EQ count by NEQ count when they have the same value.
        // If NEQ has different value it will be ignored.
        for (Map.Entry<Cell, List<Object>> entry : neqCount.entrySet()) {
            Cell entryCell = entry.getKey();
            List<Object> neqValues = entry.getValue();
            if (eqCount.containsKey(entryCell)) {
                Multiset<Object> multiset = eqCount.get(entryCell);
                for (Object value : neqValues) {
                    multiset.remove(value, 1);
                }
            }
        }

        List<Fix> result = Lists.newArrayList();
        for (Map.Entry<Cell, Multiset<Object>> entry: eqCount.entrySet()) {
            Cell cell = entry.getKey();
            Multiset<Object> multiset = entry.getValue();
            Object vfmValue = null;
            int vfmCount = 0;
            // Pick highest frequency value from the candidates
            for (Object v : multiset.elementSet()) {
                if (multiset.count(v) > vfmCount) {
                    vfmValue = v;
                    vfmCount = multiset.count(v);
                }
            }
            Fix.Builder builder = new Fix.Builder();
            result.add(
                builder
                    .left(cell)
                    .right(vfmValue)
                    .op(Operation.EQ)
                    .build()
            );
        }
        return result;
    }
}
