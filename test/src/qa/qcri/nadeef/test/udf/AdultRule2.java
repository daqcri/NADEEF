package qa.qcri.nadeef.test.udf;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.datamodel.IteratorStream;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Test rule.
 */
public class AdultRule2 extends PairTupleRule {
    @Override
    public Collection<Table> horizontalScope(
        Collection<Table> tables
    ) {
        Table table = tables.iterator().next();
        table.project("race").project("fnlwgt");
        return tables;
    }

    @Override
    public Collection<Table> block(Collection<Table> tables) {
        Table table = Iterables.get(tables, 0);
        return table.groupOn("race");
    }

    @Override
    public void iterator(
        Collection<Table> tables,
        IteratorStream<TuplePair> iteratorStream
    ) {
        Table table = tables.iterator().next();
        ArrayList<TuplePair> result = new ArrayList();
        for (int i = 0; i < table.size(); i ++) {
            for (int j = i + 1; j < table.size(); j ++) {
                Tuple left = table.get(i);
                Tuple right = table.get(j);
                if (!left.get("fnlwgt").equals(right.get("fnlwgt"))) {
                    TuplePair pair = new TuplePair(table.get(i), table.get(j));
                    iteratorStream.put(pair);
                    break;
                }
            }
        }
    }

    @Override
    public Collection<Violation> detect(TuplePair tuplePair) {
        List<Violation> result = Lists.newArrayList();

        Tuple t1 = tuplePair.getLeft();
        Tuple t2 = tuplePair.getRight();
        if (t1.getString("race").equals(t2.getString("race"))) {
            int wgt1 = ((Integer)t1.get("fnlwgt")).intValue();
            int wgt2 = ((Integer)t2.get("fnlwgt")).intValue();
            if (Math.abs(wgt1 - wgt2) <= wgt1 * 0.5) {
                Violation violation = new Violation(this.ruleName);
                violation.addCell(t1.getCell("race"));
                violation.addCell(t2.getCell("race"));
                result.add(violation);
            }
        }
        return result;
    }

    @Override
    public Collection<Fix> repair(Violation violation) {
        List<Fix> result = Lists.newArrayList();
        return result;
    }
}
