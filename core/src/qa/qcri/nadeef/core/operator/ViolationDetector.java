/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.base.Preconditions;
import qa.qcri.nadeef.core.datamodel.*;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Wrapper class for executing the violation detection.
 */
public class ViolationDetector<T>
        extends Operator<Collection<T>, Collection<Violation>> {
    private Rule rule;

    public ViolationDetector(Rule rule) {
        Preconditions.checkNotNull(rule);
        this.rule = rule;
    }

    /**
     * Execute the operator.
     *
     * @param tuples input tuples.
     * @return list of violations.
     */
    @Override
    public Collection<Violation> execute(Collection<T> tuples) throws Exception {
        ArrayList<Violation> resultCollection = new ArrayList();
        Collection<Violation> result = null;
        for (T tuple : tuples) {
            if (rule.supportOneInput()) {
                    Tuple a = (Tuple)tuple;
                    result = rule.detect(a);
            }

            if (rule.supportTwoInputs()) {
                TuplePair pair = (TuplePair)tuple;
                result = rule.detect(pair);
            }

            if (rule.supportManyInputs()) {
                TupleCollection collection = (TupleCollection)tuple;
                result = rule.detect(collection);
            }
            resultCollection.addAll(result);
        }
        return resultCollection;
    }
}



