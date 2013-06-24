/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means “Clean” in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import qa.qcri.nadeef.core.datamodel.IteratorStream;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Table;
import qa.qcri.nadeef.tools.Tracer;

import java.util.Collection;
import java.util.concurrent.*;

/**
 * Iterator.
 */
public class Iterator<E> extends Operator<Collection<Table>, Boolean> {
    //<editor-fold desc="Private members">
    private static final int MAX_THREAD_NUM = 4;
    private Rule rule;
    private ExecutorService threadExecutors;
    private CompletionService<Boolean> pool;

    //</editor-fold>

    public Iterator(Rule rule) {
        this.rule = rule;
        ThreadFactory factory =
            new ThreadFactoryBuilder().setNameFormat("iterator-pool-%d").build();
        threadExecutors = Executors.newFixedThreadPool(MAX_THREAD_NUM, factory);
        pool = new ExecutorCompletionService<>(threadExecutors);
    }

    //<editor-fold desc="IteratorCallable Class">

    /**
     * IteratorCallable is a {@link Callable} class for iteration operation on each block.
     */
    class IteratorCallable implements Callable<Boolean> {
        private IteratorStream<E> iteratorStream;
        private Collection<Table> tables;

        IteratorCallable(Collection<Table> tables) {
            this.tables = tables;
            iteratorStream = new IteratorStream<>();
        }

        IteratorCallable(Table table) {
            this(Lists.newArrayList(table));
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

            tables = null;
            return true;
        }
    }
    //</editor-fold>

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
        if (rule.supportTwoTables()) {
            // Rule runs on two tables.
            pool.submit(new IteratorCallable(tables));

            pool.take().get();
            count ++;
            setPercentage(1.0f);
        } else {
            // Rule runs on each table.
            for (Table table : tables) {
                blockSize += table.size();

                pool.submit(new IteratorCallable(table));
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
