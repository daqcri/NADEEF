/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.tools.Tracer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper class for executing the violation detection.
 */
public class ViolationDetector<T>
        extends Operator<T, Collection<Violation>> {
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
    public Collection<Violation> execute(T tuples) throws Exception {
        Stopwatch stopwatch = new Stopwatch().start();
        ArrayList<Violation> resultCollection = Lists.newArrayList();
        Collection<Violation> result = null;
        Collection<?> collections = (Collection)tuples;
        Iterator iterator = collections.iterator();
        int count = 0;
        while (iterator.hasNext()) {
            if (rule.supportOneInput()) {
                TupleCollection collection = (TupleCollection)iterator.next();
                for (int i = 0; i < collection.size(); i ++) {
                    Tuple a = collection.get(i);
                    result = rule.detect(a);
                    resultCollection.addAll(result);
                    count ++;
                }
            } else if (rule.supportTwoInputs()) {
                TuplePair pair = (TuplePair)iterator.next();
                result = rule.detect(pair);
                resultCollection.addAll(result);
                count ++;
            } else if (rule.supportManyInputs()) {
                TupleCollection collection = (TupleCollection)iterator.next();
                result = rule.detect(collection);
                resultCollection.addAll(result);
                count ++;
            }
        }

        long averageTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        Tracer.addStatEntry(Tracer.StatType.DetectCallTime, averageTime);
        Tracer.addStatEntry(Tracer.StatType.DetectCount, count);
        return resultCollection;
    }
}



