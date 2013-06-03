package qa.qcri.nadeef.test.udf;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;

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
        String zip = tuple.getString("zip");
        if (zip.equalsIgnoreCase("1183JV")) {
            if (!city.equalsIgnoreCase("amsterdam")) {
                Violation newViolation = new Violation(this.ruleName);
                newViolation.addCell(tuple.getCell("city"));
                newViolation.addCell(tuple.getCell("zip"));
                result.add(newViolation);
            }
        }
        return result;
    }

    /**
     * Repair of this rule.
     *
     *
     * @param violation violation input.
     * @return a candidate fix.
     */
    @Override
    public Collection<Fix> repair(Violation violation) {
        List<Fix> result = Lists.newArrayList();
        List<Cell> cells = Lists.newArrayList(violation.getCells());
        Fix.Builder fixBuilder = new Fix.Builder(violation);
        for (Cell cell : cells) {
            if (cell.hasColumnName("city")) {
                Fix fix = fixBuilder.left(cell).right("amsterdam").build();
                result.add(fix);
            }
        }

         return result;
    }
}
