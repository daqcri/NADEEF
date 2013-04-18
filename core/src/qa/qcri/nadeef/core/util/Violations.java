/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.util;

import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.Tuple;
import qa.qcri.nadeef.core.datamodel.Violation;

import java.util.ArrayList;
import java.util.List;

/**
 * Violation Extension helper.
 */
public class Violations {

    /**
     * Create violations from a tuple.
     * @param tuple input tuple.
     * @return collection of violations relates to this tuple.
     */
    public static List<Violation> fromTuple(String ruleId, Tuple tuple) {
        Cell[] cells = tuple.getCells();
        ArrayList<Violation> result = new ArrayList();
        for (Cell cell : cells) {
            Violation violation = new Violation();
            violation.setTupleId(tuple.getTupleId());
            violation.setRuleId(ruleId);
            violation.setCell(cell);
            violation.setAttributeValue(tuple.get(cell));
            result.add(violation);
        }
        return result;
    }
}
