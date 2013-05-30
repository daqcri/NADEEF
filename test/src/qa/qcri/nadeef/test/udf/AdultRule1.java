package qa.qcri.nadeef.test.udf;

import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;

import java.util.Collection;
import java.util.List;

/**
 * Test rule.
 */
public class AdultRule1 extends SingleTupleRule {
    /**
     * Detect rule with one tuple.
     *
     * @param tuple input tuple.
     * @return Violation set.
     */
    @Override
    public Collection<Violation> detect(Tuple tuple) {
        List<Violation> result = Lists.newArrayList();

        String martialStatus = tuple.getString("martialstatus");
        String relationship = tuple.getString("relationship");
        if (
            martialStatus.equalsIgnoreCase("Divorced") ||
            martialStatus.equalsIgnoreCase("Never-married")
        ) {
            if (
                relationship.equalsIgnoreCase("husband") ||
                relationship.equalsIgnoreCase("wife")
            ) {
                Violation violation = new Violation(this.ruleName);
                violation.addCell(tuple.getCell("martialstatus"));
                violation.addCell(tuple.getCell("relationship"));
                result.add(violation);
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
        List<Cell> violatedCells = Lists.newArrayList(violation.getCells());
        Fix.Builder fixBuilder = new Fix.Builder(violation);
        for (Cell cell : violatedCells) {
            if (cell.containsAttribute("relationship")) {
                Fix fix = fixBuilder.left(cell).right("Unmarried").build();
                result.add(fix);
            }
        }

        return result;
    }
}
