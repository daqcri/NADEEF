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

        public Detector(List<T> tupleList) {
            this.tupleList = tupleList;
        }

        /**
         * Computes a result, or throws an exception if unable to do so.
         *
         * @return detection count
         * @throws Exception if unable to compute a result
         */
        @Override
        @SuppressWarnings("unchecked")
        public Integer call() throws Exception {
            int count = 0;
            Collection<Violation> result = Lists.newArrayList();
            for (int i = 0; i < tupleList.size(); i ++) {
                T item = tupleList.get(i);
                count ++;
                Collection<Violation> violations = null;
                if (rule.supportOneInput()) {
                    Tuple tuple = (Tuple)item;
                    violations = rule.detect(tuple);
                } else if (rule.supportTwoInputs()) {
                    TuplePair pair = (TuplePair)item;
                    violations = rule.detect(pair);
                } else if (rule.supportManyInputs()) {
                    Table collection = (Table)item;
                    violations = rule.detect(collection);
                }

                if (violations != null && violations.size() > 0) {
                    result.addAll(violations);
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
    @SuppressWarnings("unchecked")
    public Collection<Violation> execute(Rule rule) throws Exception {
        this.rule = rule;
        IteratorStream iteratorStream = new IteratorStream<T>();
        resultCollection.clear();
        List<T> tupleList;
        int detectCount = 0;
        int detectThread = 0;
        long elapsedTime = 0l;

        while (true) {
            tupleList = (List<T>)iteratorStream.poll();
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

        Tracer.putStatsEntry(Tracer.StatType.DetectCallTime, elapsedTime);
        Tracer.putStatsEntry(Tracer.StatType.DetectCount, detectCount);
        Tracer.putStatsEntry(Tracer.StatType.DetectThreadCount, detectThread);

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