/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.udf;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;

import java.util.Collection;

/**
 * My custom rule with pair detection.
 */
public class MyRule2 extends PairTupleRule {
    /**
     * Detect rule with one tuple.
     *
     * @param tuples input tuple.
     * @return Violation set.
     */
    @Override
    public Collection<Violation> detect(TuplePair tuples) {
        Tuple left = tuples.getLeft();
        Tuple right = tuples.getRight();

        Column city = new Column(tableNames.get(0), "city");

        Violation violation = new Violation(id);
        if (!left.get(city).equals(right.get(city))) {
            violation.addTuple(left);
			violation.addTuple(right);
        }

        return Lists.newArrayList(violation);
    }

    /**
     * Repair of this rule.
     *
     * @param violation violation input.
     * @return a candidate fix.
     */
    @Override
    public Collection<Fix> repair(Violation violation) {
        return null;
    }

    @Override
    public Collection<TupleCollection> scope(Collection<TupleCollection> tupleCollections) {
        TupleCollection collection = tupleCollections.iterator().next();
        String tableName = tableNames.get(0);
        collection.project(new Column(tableName, "city"))
                .project(new Column(tableName, "zip"))
                .filter(SimpleExpression.newEqual(new Column(tableName, "zip"), "1183JV"));
        return tupleCollections;
    }
}
