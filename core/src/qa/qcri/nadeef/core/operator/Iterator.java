/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.operator;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.IteratorOutput;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.TupleCollection;
import qa.qcri.nadeef.tools.Tracer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * Iterator.
 */
public class Iterator<E> extends Operator<Collection<TupleCollection>, Boolean> {
    private static final int MAX_THREAD_NUM = 10;

    private Rule rule;

    private ExecutorService threadExecutors = Executors.newFixedThreadPool(MAX_THREAD_NUM);
    private CompletionService<Boolean> pool =
        new ExecutorCompletionService<Boolean>(threadExecutors);

    public Iterator(Rule rule) {
        this.rule = rule;
    }

    class IteratorCallable implements Callable<Boolean> {
        private IteratorOutput<E> iteratorOutput;
        private TupleCollection tupleCollection;

        IteratorCallable(TupleCollection tupleCollection, IteratorOutput<E> iteratorOutput) {
            this.iteratorOutput = iteratorOutput;
            this.tupleCollection = tupleCollection;
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        @Override
        public Boolean call() throws Exception {
            rule.iterator(tupleCollection, iteratorOutput);
            iteratorOutput.flush();
            return true;
        }
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
        List<IteratorOutput> iteratorOutputs = Lists.newArrayList();

        for (TupleCollection tupleCollection : tupleCollections) {
            count += tupleCollection.size();
            IteratorOutput<E> iteratorOutput = new IteratorOutput<E>();
            iteratorOutputs.add(iteratorOutput);

            pool.submit(
                new IteratorCallable(
                    tupleCollection, new IteratorOutput<E>()
                )
            );
        }

        for (TupleCollection tupleCollection : tupleCollections) {
            pool.take().get();
        }

        for (TupleCollection tupleCollection : tupleCollections) {
            tupleCollection.recycle();
        }

        // mark the end of the iteration output
        IteratorOutput.markEnd();

        Tracer.addStatEntry(
            Tracer.StatType.IteratorTime,
            stopwatch.elapsed(TimeUnit.MILLISECONDS)
        );

        Tracer.addStatEntry(Tracer.StatType.IterationCount, count);
        stopwatch.stop();
        return true;
    }

    @Override
    public void finalize() {
        if (!threadExecutors.isShutdown()) {
            threadExecutors.shutdownNow();
        }
    }
}
