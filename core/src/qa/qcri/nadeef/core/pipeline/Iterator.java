/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
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
import com.google.common.util.concurrent.*;
import qa.qcri.nadeef.core.datamodel.IteratorStream;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Table;
import qa.qcri.nadeef.tools.Tracer;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.concurrent.*;

/**
 * Iterator.
 */
public class Iterator<E> extends Operator<Collection<Table>, Boolean> {
    //<editor-fold desc="Private members">
    private static final int MAX_THREAD_NUM = Runtime.getRuntime().availableProcessors();
    private Rule rule;
    private int totalBlockSize;
    private int blockCount;

    //</editor-fold>

    public Iterator(Rule rule) {
        this.rule = rule;
    }

    //<editor-fold desc="IteratorCallable and Callback Class">

    /**
     * Callback class for progress report.
     */
    class IteratorCallback implements FutureCallback<Integer> {
        private int tableSize;

        public IteratorCallback(int tableSize) {
            this.tableSize = tableSize;
        }
        @Override
        public void onSuccess(Integer integer) {
            synchronized (Iterator.class) {
                totalBlockSize += integer;
                blockCount ++;
                setPercentage(blockCount / tableSize);
            }
        }

        @Override
        public void onFailure(Throwable throwable) {}
    }

    /**
     * IteratorCallable is a {@link Callable} class for iteration operation on each block.
     */
    class IteratorCallable<T> implements Callable<Integer> {
        private IteratorStream<E> iteratorStream;
        private WeakReference<T> ref;

        IteratorCallable(T tables) {
            ref = new WeakReference<>(tables);
            iteratorStream = new IteratorStream<>();
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return computed result
         * @throws Exception if unable to compute a result
         */
        @Override
        @SuppressWarnings("unchecked")
        public Integer call() throws Exception {
            T instance = ref.get();
            if (instance == null) {
                throw new RuntimeException("Tables have been freed.");
            }

            Collection<Table> value;
            if (Table.class.isAssignableFrom(instance.getClass())) {
                value = Lists.newArrayList((Table)instance);
            } else {
                value = (Collection<Table>)instance;
            }
            rule.iterator(value, iteratorStream);
            iteratorStream.flush();

            // return the tuple total count
            int size = 0;
            for (Table table : value) {
                size += table.size();
            }
            return size;
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
    @SuppressWarnings("unchecked")
    public Boolean execute(Collection<Table> tables) {
        Tracer tracer = Tracer.getTracer(Iterator.class);
        ThreadFactory factory =
            new ThreadFactoryBuilder().setNameFormat("iterator-pool-%d").build();
        ExecutorService executor = Executors.newFixedThreadPool(MAX_THREAD_NUM, factory);
        ListeningExecutorService service = MoreExecutors.listeningDecorator(executor);

        Stopwatch stopwatch = new Stopwatch().start();
        totalBlockSize = 0;
        blockCount = 0;

        try {
            if (rule.supportTwoTables()) {
                // Rule runs on two tables.
                ListenableFuture<Integer> future = service.submit(new IteratorCallable(tables));
                totalBlockSize = future.get();
                blockCount ++;
                setPercentage(1.0f);
            } else {
                // Rule runs on each table.
                for (Table table : tables) {
                    ListenableFuture<Integer> future =
                        service.submit(new IteratorCallable(table));
                    Futures.addCallback(future, new IteratorCallback(tables.size()));
                }
            }

            // wait until all the tasks are finished
            service.shutdown();
            while (!service.awaitTermination(10l, TimeUnit.MINUTES));

            // recycle the collection when dealing with pairs. This is mainly used to remove refs.
            if (rule.supportTwoInputs()) {
                for (Table table : tables) {
                    table.recycle();
                }
            }

            // mark the end of the iteration output
            IteratorStream.markEnd();
        } catch (InterruptedException ex) {
            tracer.err("Iterator is interrupted.", ex);
        } catch (ExecutionException ex) {
            tracer.err("Iterator execution failed.", ex);
        } finally {
            executor.shutdown();
        }

        Tracer.putStatsEntry(
            Tracer.StatType.IteratorTime,
            stopwatch.elapsed(TimeUnit.MILLISECONDS)
        );

        Tracer.putStatsEntry(Tracer.StatType.IterationCount, totalBlockSize);
        stopwatch.stop();
        return true;
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
