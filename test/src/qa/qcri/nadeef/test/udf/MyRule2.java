/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.udf;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;

import java.util.Collection;
import java.util.List;

/**
 * My custom rule with pair detection.
 */
public class MyRule2 extends Rule<TuplePair> {
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

        String tableName = tableNames.get(0);
        Column city = new Column(tableName, "city");
        Column zip = new Column(tableName, "zip");

        Violation violation = new Violation(id);
        if (!left.get(city).equals(right.get(city))) {
            violation.addTuple(left);
        }

        return Lists.newArrayList(violation);
    }
}
