/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Preconditions;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.operator.*;

import java.util.Collection;
import java.util.List;

/**
 * CleanPlan execution logic. It assembles the right pipeline based on the clean plan and
 * start the execution.
 */
public class CleanExecutor {
    private static CleanPlan cleanPlan;

    /**
     * Constructor.
     * @param cleanPlan clean plan.
     */
    public CleanExecutor(CleanPlan cleanPlan) {
        Preconditions.checkNotNull(cleanPlan);
        this.cleanPlan = cleanPlan;
    }

    /**
     * Runs the cleaning execution.
     */
    public void run() {
        List<Rule> rules = cleanPlan.getRules();
        Flow[] flows = new Flow[rules.size()];
        NodeCacheManager cacheManager = NodeCacheManager.getInstance();
        // assembly the flow.
        for (int i = 0; i < flows.length; i ++)  {
            flows[i] = new Flow();
            Rule rule = rules.get(i);
            String inputKey = cacheManager.put(rule);
            flows[i].setInputKey(inputKey);
            if (rule.supportTwoInputs()) {
                flows[i].addNode(
                    new Node(new Deseralizer<TupleCollectionPair>(cleanPlan), "deserializer")
                );
                flows[i].addNode(new Node(new PairQueryEngine(rule), "query"));
                flows[i].addNode(new Node(new TupleCollectionPairIterator(), "iterator"));
                flows[i].addNode(new Node(new ViolationDetector<TuplePair>(rule), "detector"));
            } else {
                flows[i].addNode(
                    new Node(new Deseralizer<TupleCollection>(cleanPlan), "deserializer")
                );
                flows[i].addNode(new Node(new QueryEngine(rule), "query"));
                flows[i].addNode(new Node(new TupleCollectionIterator(), "iterator"));
                flows[i].addNode(
                    new Node(new ViolationDetector<TupleCollection>(rule), "detector")
                );
            }

            flows[i].addNode(new Node(new ViolationExport(cleanPlan), "export"));
        }

        // TODO: run multiple rules in parallel in process / thread.
        for (Flow flow : flows) {
            flow.start();
        }
    }

    /**
     * Gets the <code>CleanPlan</code>.
     * @return the <code>CleanPlan</code>.
     */
    public static CleanPlan getCurrentCleanPlan() {
        return cleanPlan;
    }
}
