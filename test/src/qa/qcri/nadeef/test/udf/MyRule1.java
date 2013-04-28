package qa.qcri.nadeef.test.udf;

import qa.qcri.nadeef.core.datamodel.Fix;
import qa.qcri.nadeef.core.datamodel.SingleTupleRule;
import qa.qcri.nadeef.core.datamodel.Tuple;
import qa.qcri.nadeef.core.datamodel.Violation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Test rule.
 */
public class MyRule1 extends SingleTupleRule {
    /**
     * Detect rule with one tuple.
     *
     * @param tuple input tuple.
     * @return Violation set.
     */
    @Override
    public Collection<Violation> detect(Tuple tuple) {
        List<Violation> result = new ArrayList();
        String city = tuple.getString("city");

        if (!city.equalsIgnoreCase("amsterdam")) {
            Violation newViolation = new Violation(this.id);
            newViolation.addCell(tuple.getCell("city"));
            newViolation.addCell(tuple.getCell("zip"));
            result.add(newViolation);
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
    public Fix repair(Violation violation) {
        return null;
    }
}
