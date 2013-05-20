/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.base.Stopwatch;
import qa.qcri.nadeef.core.datamodel.IteratorOutput;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.TupleCollection;
import qa.qcri.nadeef.tools.Tracer;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

/**
 * Iterator.
 */
public class Iterator extends Operator<Collection<TupleCollection>, Boolean> {
    private Rule rule;
    private IteratorOutput output;

    public Iterator(Rule rule, IteratorOutput output) {
        this.rule = rule;
        this.output = output;
    }

    /**
     * Execute the operator.
     *
     * @param tupleCollections input object.
     * @return output object.
     */
    @Override
    public Boolean execute(Collection<TupleCollection> tupleCollections) throws Exception {
        int count = 0;
        Stopwatch stopwatch = new Stopwatch().start();
        for (TupleCollection tupleCollection : tupleCollections) {
            count += tupleCollection.size();
            rule.iterator(tupleCollection, output);
        }

        // mark the end of the iteration output
        output.markEnd();
        Tracer.addStatEntry(
            Tracer.StatType.IteratorTime,
            stopwatch.elapsed(TimeUnit.MILLISECONDS)
        );

        Tracer.addStatEntry(Tracer.StatType.IterationCount, count);
        stopwatch.stop();
        return true;
    }
}
