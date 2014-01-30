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

package qa.qcri.nadeef.test.udf;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.tools.Metrics;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class VehicleRule1 extends PairTupleRule {
    @Override
    public void initialize(String id, List<String> tableNames) {
        super.initialize(id, tableNames);
    }

    /**
     * Detect method.
     * @param tuplePair tuple pair.
     * @return violation set.
     */
    @Override
    public Collection<Violation> detect(TuplePair tuplePair) {
        List<Violation> result = new ArrayList<>();
        Tuple left = tuplePair.getLeft();
        Tuple right = tuplePair.getRight();

        if (
            Metrics.getEqual(
                getValue(tuplePair, "vehicles", "title", 0),
                getValue(tuplePair, "vehicles", "title", 1)
            )== 1.0 &&
            Metrics.getEqual(
                getValue(tuplePair, "vehicles", "username", 0),
                getValue(tuplePair, "vehicles", "username", 1)
            ) ==1.0
        ) {
            Violation violation = new Violation(getRuleName());
            violation.addCell(left.getCell("id"));
            violation.addCell(right.getCell("id"));
            result.add(violation);
        }

        return result;
    }

    /**
     * Repair of this rule.
     *
     * @param violation violation input.
     * @return a candidate fix.
     */
    @Override
    public Collection<Fix> repair(Violation violation) {
        List<Fix> result = Lists.newArrayList();
        List<Cell> cells = Lists.newArrayList(violation.getCells());

        Fix.Builder fixBuilder = new Fix.Builder(violation);
        Fix fix = fixBuilder.left(cells.get(0)).right(cells.get(1)).build();
        result.add(fix);
        return result;
    }

    private String getValue(TuplePair pair, String tableName, String column, int isLeft) {
        Tuple left = pair.getLeft();
        Tuple right = pair.getRight();
        String result;
        if (isLeft == 0) {
            if (left.isFromTable(tableName)) {
                Object obj = left.get(column);
                result = obj != null ? obj.toString() : null;
            } else {
                Object obj = right.get(column);
                result = obj != null ? obj.toString() : null;
            }
        } else {
            if (right.isFromTable(tableName)) {
                Object obj = right.get(column);
                result = obj != null ? obj.toString() : null;
            } else {
                Object obj = left.get(column);
                result = obj != null ? obj.toString() : null;
            }
        }
        return result;
    }
}