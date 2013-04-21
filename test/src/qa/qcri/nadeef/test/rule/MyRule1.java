package qa.qcri.nadeef.test.rule;

import qa.qcri.nadeef.core.datamodel.Column;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Tuple;
import qa.qcri.nadeef.core.datamodel.Violation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Test rule.
 */
public class MyRule1 extends Rule<Tuple> {
    /**
     * Constructor. Checks for which signatures are implemented.
     */
    public MyRule1(String id, List<String> tableNames) {
        super(id, tableNames);
    }

    /**
     * Detect rule with one tuple.
     *
     * @param tuple input tuple.
     * @return Violation set.
     */
    @Override
    public Collection<Violation> detect(Tuple tuple) {
        Column zipColumn = new Column("location", "zip");
        String zip = tuple.getString(zipColumn);
        if (!zip.equalsIgnoreCase("1183JV")) {
            return new ArrayList();
        }

        List<Violation> result = new ArrayList();
        Column cityColumn = new Column("location", "city");
        String city = tuple.getString(cityColumn);

        if (!city.equalsIgnoreCase("amsterdam")) {
            Violation newViolation = new Violation(this.id);
            newViolation.addCell(tuple, cityColumn);
            newViolation.addCell(tuple, zipColumn);
            result.add(newViolation);
        }
        return result;
    }
}
