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
 */
public class MyRule6 extends SingleTupleRule {
    /**
     * Detect rule with one tuple.
     *
     * @param tuple input tuple.
     * @return Violation set.
     */
    @Override
    public Collection<Violation> detect(Tuple tuple) {
        List<Violation> result = Lists.newArrayList();
        if (tuple.getCell("CC").getValue().equals("31") &&
            (
                !tuple.getCell("country").getValue().equals("Netherlands") ||
                !tuple.getCell("country").getValue().equals("Holland"))
            )
        {
            Violation violation = new Violation(ruleName);
            violation.addCell(tuple.getCell("cc"));
            violation.addCell(tuple.getCell("country"));
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
        Fix.Builder builder = new Fix.Builder();
        Cell countryCell = violation.getCell("tran1", "country");
        Fix fix = builder.left(countryCell).right("Netherlands").build();
        return Lists.newArrayList(fix);
    }
}
