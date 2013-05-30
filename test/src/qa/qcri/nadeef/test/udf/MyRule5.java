/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.test.udf;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;

import java.util.Collection;

/**
 * Pair table test.
 */
public class MyRule5 extends PairTupleRule {

    /**
     * Detect rule with pair tuple.
     *
     * @param pair input tuple pair.
     * @return Violation set.
     */
    @Override
    public Collection<Violation> detect(TuplePair pair) {
        Tuple bankTuple;
        Tuple tranTuple;
        if (pair.getLeft().getTableName().equals("bank1")) {
            bankTuple = pair.getLeft();
            tranTuple = pair.getRight();
        } else {
            bankTuple = pair.getRight();
            tranTuple = pair.getLeft();
        }

        if (
            bankTuple.get("FN").equals(tranTuple.get("FN")) &&
            bankTuple.get("LN").equals(tranTuple.get("LN")) &&
            bankTuple.get("ST").equals(tranTuple.get("str")) &&
            bankTuple.get("city").equals(tranTuple.get("city")) &&
            !bankTuple.get("tel").equals(tranTuple.get("phn"))
        ) {
            Violation violation = new Violation(ruleName);
            violation.addCell(bankTuple.getCell("FN"));
            violation.addCell(bankTuple.getCell("LN"));
            violation.addCell(bankTuple.getCell("ST"));
            violation.addCell(bankTuple.getCell("city"));
            violation.addCell(bankTuple.getCell("tel"));
            violation.addCell(tranTuple.getCell("FN"));
            violation.addCell(tranTuple.getCell("LN"));
            violation.addCell(tranTuple.getCell("str"));
            violation.addCell(tranTuple.getCell("city"));
            violation.addCell(tranTuple.getCell("phn"));
            return Lists.newArrayList(violation);
        }
        return null;
    }

    /**
     * Repair of this rule.
     *
     * @param violation violation input.
     * @return a candidate fix.
     */
    @Override
    public Collection<Fix> repair(Violation violation) {
        Fix.Builder builder = new Fix.Builder();
        Cell tran = violation.getCell("tran1", "phn");
        Cell bank = violation.getCell("bank1", "tel");
        Fix fix = builder.left(tran).right(bank).build();
        return Lists.newArrayList(fix);
    }
}
