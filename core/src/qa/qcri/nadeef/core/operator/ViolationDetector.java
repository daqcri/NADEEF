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
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Wrapper class for executing the violation detection.
 */
public class ViolationDetector<T>
    extends Operator<IteratorOutput<T>, Collection<Violation>> {
    private Rule rule;

    public ViolationDetector(Rule rule) {
        Preconditions.checkNotNull(rule);
        this.rule = rule;
    }

    /**
     * Execute the operator.
     *
     * @param iteratorOutput Iterator output.
     * @return list of violations.
     */
    @Override
    public Collection<Violation> execute(IteratorOutput<T> iteratorOutput) throws Exception {
        Stopwatch stopwatch = new Stopwatch();
        ArrayList<Violation> resultCollection = Lists.newArrayList();
        Collection<Violation> result = null;
        List<T> tupleList = null;
        int count = 0;
        long elapsedTime = 0l;
        while (true) {
            tupleList = iteratorOutput.poll();
            if (tupleList.size() == 0) {
                break;
            }

            for (int i = 0; i < tupleList.size(); i ++) {
                T item = tupleList.get(i);
                if (elapsedTime == 0) {
                    stopwatch.start();
                }
                if (rule.supportOneInput()) {
                    TupleCollection collection = (TupleCollection)item;
                    for (int j = 0; j < collection.size(); j ++) {
                        Tuple a = collection.get(i);
                        result = rule.detect(a);
                        resultCollection.addAll(result);
                        count ++;
                        if (elapsedTime == 0) {
                            elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                        }
                    }
                } else if (rule.supportTwoInputs()) {
                    TuplePair pair = (TuplePair)item;
                    result = rule.detect(pair);
                    resultCollection.addAll(result);
                    count ++;
                } else if (rule.supportManyInputs()) {
                    TupleCollection collection = (TupleCollection)item;
                    result = rule.detect(collection);
                    resultCollection.addAll(result);
                    count ++;
                }
                if (elapsedTime == 0) {
                    elapsedTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                }
            }
        }

        Tracer.addStatEntry(
            Tracer.StatType.DetectCallTime,
            elapsedTime
        );

        stopwatch.stop();
        Tracer.addStatEntry(Tracer.StatType.DetectCount, count);
        return resultCollection;
    }

    private void detect(T item) {

    }
}