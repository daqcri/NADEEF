/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.IteratorStream;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Table;
import qa.qcri.nadeef.tools.Tracer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * Iterator.
 */
class Iterator<E> extends Operator<Collection<Table>, Boolean> {
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
        private Collection<Table> tables;

        IteratorCallable(Collection<Table> tables, IteratorStream<E> iteratorStream) {
            this.iteratorStream = iteratorStream;
            this.tables = tables;
        }

        IteratorCallable(Table table, IteratorStream<E> iteratorStream) {
            this.iteratorStream = iteratorStream;
            this.tables = Lists.newArrayList(table);
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        @Override
        @SuppressWarnings("unchecked")
        public Boolean call() throws Exception {
            rule.iterator(tables, iteratorStream);
            iteratorStream.flush();
            return true;
        }
    }

    /**
     * Iterator operator execution.
     *
     * @param tables input tables.
     * @return iteration output.
     */
    @Override
    public Boolean execute(Collection<Table> tables) throws Exception {
        int blockSize = 0;
        int count = 0;
        Stopwatch stopwatch = new Stopwatch().start();
        List<IteratorStream> iteratorStreams = Lists.newArrayList();

        if (rule.supportTwoTables()) {
            // Rule runs on two tables.
            IteratorStream<E> iteratorStream = new IteratorStream<E>();
            iteratorStreams.add(iteratorStream);

            pool.submit(
                new IteratorCallable(
                    tables, new IteratorStream<E>()
                )
            );

            pool.take().get();
            count ++;
            setPercentage(1.0f);
        } else {
            // Rule runs on each table.
            for (Table table : tables) {
                blockSize += table.size();
                IteratorStream<E> iteratorStream = new IteratorStream<E>();
                iteratorStreams.add(iteratorStream);

                pool.submit(
                    new IteratorCallable(
                        table, new IteratorStream<E>()
                    )
                );
            }

            for (Table table : tables) {
                pool.take().get();
                count ++;
                setPercentage(count / tables.size());
            }
        }

        // recycle the collection when dealing with pairs. This is mainly used to remove views.
        if (rule.supportTwoInputs()) {
            for (Table table : tables) {
                table.recycle();
            }
        }

        // mark the end of the iteration output
        IteratorStream.markEnd();

        Tracer.putStatsEntry(
            Tracer.StatType.IteratorTime,
            stopwatch.elapsed(TimeUnit.MILLISECONDS)
        );

        Tracer.putStatsEntry(Tracer.StatType.IterationCount, blockSize);
        stopwatch.stop();
        return true;
    }

    /**
     * Finalize the Iterator.
     */
    @Override
    public void finalize() {
        if (!threadExecutors.isShutdown()) {
            threadExecutors.shutdownNow();
        }
    }

    /**
     * Interrupt is called when the Iterator is stopped in the middle. A typical scenario is
     * that there is exception happened during iterator execution.
     */
    @Override
    public void interrupt() {
        IteratorStream.markEnd();
    }

    /**
     * Reset is called before iterator starts to execute.
     */
    @Override
    public void reset() {
        IteratorStream.clear();
    }
}
