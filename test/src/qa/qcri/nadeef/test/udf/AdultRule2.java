package qa.qcri.nadeef.test.udf;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Test rule.
 */
public class AdultRule2 extends PairTupleRule {
    @Override
    public Collection<TupleCollection> scope(Collection<TupleCollection> tupleCollections) {
        TupleCollection tupleCollection = tupleCollections.iterator().next();
        tupleCollection
            .project(new Column(tableNames.get(0), "race"))
            .project(new Column(tableNames.get(0), "fnlwgt"));
        return tupleCollections;
    }

    @Override
    public Collection<TuplePair> group(Collection<TupleCollection> tupleCollections) {
        ArrayList<TuplePair> result = new ArrayList();
        TupleCollection tupleCollection = Iterables.get(tupleCollections, 0);
        Collection<TupleCollection> groupResult =
            tupleCollection.groupOn(new Column(tableNames.get(0), "race"));
        for (TupleCollection tuples : groupResult) {
            for (int i = 0; i < tuples.size(); i ++) {
                for (int j = i + 1; j < tuples.size(); j ++) {
                    TuplePair pair = new TuplePair(tuples.get(i), tuples.get(j));
                    result.add(pair);
                }
            }
        }
        return result;
    }

    @Override
    public Collection<Violation> detect(TuplePair tuplePair) {
        List<Violation> result = Lists.newArrayList();

        Tuple t1 = tuplePair.getLeft();
        Tuple t2 = tuplePair.getRight();
        if (t1.getString("race").equals(t2.getString("race"))) {
            int wgt1 = ((Integer)t1.get("fnlwgt")).intValue();
            int wgt2 = ((Integer)t2.get("fnlwgt")).intValue();
            if (Math.abs(wgt1 - wgt2) >= wgt1 * 0.5) {
                Violation violation = new Violation(this.id);
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
