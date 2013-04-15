/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Preconditions;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.Tuple;
import qa.qcri.nadeef.core.datamodel.TuplePair;
import qa.qcri.nadeef.core.operator.*;

import java.util.Collection;
import java.util.List;

/**
 * CleanPlan execution logic. It assembles the right pipeline based on the clean plan and
 * start the execution.
 */
public class CleanExecutor {
    private CleanPlan cleanPlan;

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
            if (cleanPlan.isSourceDataBase()) {
                flows[i].addNode(new Node(new SQLDeseralizer(cleanPlan), "deseralizer"));
            }

            if (rule.supportTwoInputs()) {
                flows[i].addNode(new Node(new TuplePairIterator(), "iterator"));
                flows[i].addNode(new Node(new ViolationDetector<TuplePair>(rule), "detector"));
            } else if (rule.supportManyInputs()) {
                flows[i].addNode(new Node(new TupleCollectionIterator(), "iterator"));
                flows[i].addNode(
                    new Node(new ViolationDetector<Collection<Tuple>>(rule), "detector")
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
    public CleanPlan getCleanPlan() {
        return cleanPlan;
    }
}
