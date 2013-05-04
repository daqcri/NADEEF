/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.Rule;
import qa.qcri.nadeef.core.datamodel.TupleCollection;
import qa.qcri.nadeef.core.datamodel.TuplePair;
import qa.qcri.nadeef.core.operator.*;
import qa.qcri.nadeef.tools.Tracer;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * CleanPlan execution logic. It assembles the right pipeline based on the clean plan and
 * start the execution.
 */
public class CleanExecutor {
    private static Tracer tracer = Tracer.getTracer(CleanExecutor.class);
    private CleanPlan cleanPlan;
    private NodeCacheManager cacheManager;

    /**
     * Constructor.
     * @param cleanPlan clean plan.
     */
    public CleanExecutor(CleanPlan cleanPlan) {
        this.cleanPlan = Preconditions.checkNotNull(cleanPlan);
        this.cacheManager = NodeCacheManager.getInstance();
    }

    /**
     * Runs the violation repair execution.
     */
    public Flow[] detect() {
        List<Rule> rules = cleanPlan.getRules();
        Flow[] flows = new Flow[rules.size()];

        try {
            // assemble the flow.
            for (int i = 0; i < flows.length; i ++)  {
                flows[i] = new Flow();
                Rule rule = rules.get(i);
                String inputKey = cacheManager.put(rule);
                flows[i].setInputKey(inputKey);
                flows[i].addNode(new Node(new SourceDeserializer(cleanPlan), "deserializer"));
                flows[i].addNode(new Node(new QueryEngine(rule), "query"));
                if (rule.supportTwoInputs()) {
                    // the case where the rule is working on multiple tables (2).
                    flows[i].addNode(new Node(new ViolationDetector<TuplePair>(rule), "detector"));
                } else {
                    flows[i].addNode(
                        new Node(new ViolationDetector<TupleCollection>(rule), "detector")
                    );
                }

                flows[i].addNode(new Node(new ViolationExport(cleanPlan), "export"));
            }
        } catch (Exception ex) {
            Tracer tracer = Tracer.getTracer(CleanExecutor.class);
            tracer.err("Exception happens during assembling the pipeline " + ex.getMessage());
            if (Tracer.isInfoOn()) {
                ex.printStackTrace();
            }
            return null;
        }

        // TODO: run multiple rules in parallel in process / thread.
        for (int i = 0; i < flows.length; i ++) {
            Stopwatch stopwatch = new Stopwatch().start();
            Flow flow = flows[i];
            flow.start();
            Tracer.addStatEntry(
                "Rule [" + rules.get(i).getId() + "] Detection time",
                stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms."
            );
        }
        return flows;
    }

    /**
     * Runs the violation detection execution.
     */
    public Flow[] repair() {
        List<Rule> rules = cleanPlan.getRules();
        Flow[] flows = new Flow[rules.size()];
        NodeCacheManager cacheManager = NodeCacheManager.getInstance();

        try {
            // assemble the flow.
            for (int i = 0; i < flows.length; i ++)  {
                flows[i] = new Flow();
                Rule rule = rules.get(i);
                String inputKey = cacheManager.put(rule);
                flows[i].setInputKey(inputKey);
                flows[i].addNode(new Node(new ViolationDeserializer(), "violation_deserializer"));
                flows[i].addNode(new Node(new ViolationRepair(rule), "violation_repair"));
                flows[i].addNode(new Node(new FixExport(cleanPlan), "fix export"));
            }
        } catch (Exception ex) {
            tracer.err("Exception happens during assembling the pipeline " + ex.getMessage());
            if (Tracer.isInfoOn()) {
                ex.printStackTrace();
            }
            return null;
        }

        // TODO: run multiple rules in parallel in process / thread.
        for (int i = 0; i < flows.length; i ++) {
            Stopwatch stopwatch = new Stopwatch().start();
            Flow flow = flows[i];
            flow.start();
            Tracer.addStatEntry(
                "Rule [" + rules.get(i).getId() + "] Generate Candidate Fix time",
                stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms."
            );
        }

        return flows;
    }

    public void apply(Rule rule) {
        // Apply Fix decisions.
        Stopwatch stopwatch = new Stopwatch().start();
        Flow eq = new Flow();
        String inputKey = cacheManager.put(rule);
        eq.setInputKey(inputKey);
        eq.addNode(new Node(new FixDeserializer(), "fix deserializer"));
        eq.addNode(new Node(new FixDecisionMaker(), "eq"));
        eq.addNode(new Node(new Updater(cleanPlan), "updater"));
        eq.start();
        Tracer.addStatEntry(
            "EQ running time", stopwatch.elapsed(TimeUnit.MILLISECONDS) + "ms."
        );
    }
}
