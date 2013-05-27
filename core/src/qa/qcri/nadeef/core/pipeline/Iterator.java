/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.IteratorStream;
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
        private IteratorStream<E> iteratorStream;
        private TupleCollection tupleCollection;

        IteratorCallable(TupleCollection tupleCollection, IteratorStream<E> iteratorStream) {
            this.iteratorStream = iteratorStream;
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
            rule.iterator(tupleCollection, iteratorStream);
            iteratorStream.flush();
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
        int blockSize = 0;
        int count = 0;
        Stopwatch stopwatch = new Stopwatch().start();
        List<IteratorStream> iteratorStreams = Lists.newArrayList();

        for (TupleCollection tupleCollection : tupleCollections) {
            blockSize += tupleCollection.size();
            IteratorStream<E> iteratorStream = new IteratorStream<E>();
            iteratorStreams.add(iteratorStream);

            pool.submit(
                new IteratorCallable(
                    tupleCollection, new IteratorStream<E>()
                )
            );
        }

        for (TupleCollection tupleCollection : tupleCollections) {
            pool.take().get();
            count ++;
            setPercentage(count / tupleCollections.size());
        }

        // recycle the collection when dealing with pairs. This is mainly used to remove views.
        if (rule.supportTwoInputs()) {
            for (TupleCollection tupleCollection : tupleCollections) {
                tupleCollection.recycle();
            }
        }

        // mark the end of the iteration output
        IteratorStream.markEnd();

        Tracer.addStatEntry(
            Tracer.StatType.IteratorTime,
            stopwatch.elapsed(TimeUnit.MILLISECONDS)
        );

        Tracer.addStatEntry(Tracer.StatType.IterationCount, blockSize);
        stopwatch.stop();
        return true;
    }

    @Override
    public void finalize() {
        if (!threadExecutors.isShutdown()) {
            threadExecutors.shutdownNow();
        }
    }

    @Override
    public void interrupt() {
        IteratorStream.markEnd();
    }
}
