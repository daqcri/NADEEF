/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.tools.Tracer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * Wrapper class for executing the violation detection.
 */
public class ViolationDetector<T>
    extends Operator<Rule, Collection<Violation>> {
    private static final int MAX_THREAD_NUM = 20;

    private Rule rule;
    private Collection<Violation> resultCollection;

    private ExecutorService threadExecutors = Executors.newFixedThreadPool(MAX_THREAD_NUM);
    private CompletionService<Integer> pool =
        new ExecutorCompletionService<Integer>(threadExecutors);

    /**
     * Violation detector constructor.
     * @param rule rule.
     */
    public ViolationDetector(Rule rule) {
        Preconditions.checkNotNull(rule);
        resultCollection = Lists.newArrayList();
    }

    /**
     * Detector callable class.
     */
    class Detector<T> implements Callable<Integer> {
        private List<T> tupleList;

        public Detector(List tupleList) {
            this.tupleList = tupleList;
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return detection count
         * @throws Exception if unable to compute a result
         */
        @Override
        public Integer call() throws Exception {
            int count = 0;
            Collection<Violation> result = Lists.newArrayList();
            for (int i = 0; i < tupleList.size(); i ++) {
                T item = tupleList.get(i);
                if (rule.supportOneInput()) {
                    TupleCollection collection = (TupleCollection)item;
                    for (int j = 0; j < collection.size(); j ++) {
                        Tuple tuple = collection.get(j);
                        result.addAll(rule.detect(tuple));
                        count ++;
                    }
                } else if (rule.supportTwoInputs()) {
                    TuplePair pair = (TuplePair)item;
                    result.addAll(rule.detect(pair));
                    count ++;
                } else if (rule.supportManyInputs()) {
                    TupleCollection collection = (TupleCollection)item;
                    result.addAll(rule.detect(collection));
                    count ++;
                }
            }

            synchronized (ViolationDetector.class) {
                resultCollection.addAll(result);
            }
            return count;
        }
    }

    /**
     * Execute the operator.
     *
     * @param rule rule.
     * @return list of violations.
     */
    @Override
    public Collection<Violation> execute(Rule rule) throws Exception {
        this.rule = rule;
        IteratorStream iteratorStream = new IteratorStream<T>();
        resultCollection.clear();
        List<T> tupleList = null;
        int detectCount = 0;
        int detectThread = 0;
        long elapsedTime = 0l;

        while (true) {
            tupleList = iteratorStream.poll();
            if (tupleList.size() == 0) {
                break;
            }

            detectThread ++;
            pool.submit(new Detector<T>(tupleList));
        }

        for (int i = 0; i < detectThread; i ++) {
            setPercentage(i / detectThread);
            detectCount += pool.take().get();
        }

        Tracer.addStatEntry(Tracer.StatType.DetectCallTime, elapsedTime);
        Tracer.addStatEntry(Tracer.StatType.DetectCount, detectCount);
        Tracer.addStatEntry(Tracer.StatType.DetectThreadCount, detectThread);

        return resultCollection;
    }

    @Override
    public void finalize() throws Throwable {
        super.finalize();
        if (!threadExecutors.isShutdown()) {
            threadExecutors.shutdownNow();
        }
    }
}