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
    private Flow[] detectFlows;
    private Flow[] repairFlows;
    private Flow[] updateFlows;

    //<editor-fold desc="Constructor">
    /**
     * Constructor.
     * @param cleanPlan clean plan.
     */
    public CleanExecutor(CleanPlan cleanPlan) {
        this.cleanPlan = Preconditions.checkNotNull(cleanPlan);
        this.cacheManager = NodeCacheManager.getInstance();
        assembleFlow();
    }
    //</editor-fold>

    //<editor-fold desc="Public methods">
    public Object getDetectOutput() {
        String key = detectFlows[detectFlows.length - 1].getLastOutputKey();
        return cacheManager.get(key);
    }

    public Object getRepairOutput() {
        String key = repairFlows[detectFlows.length - 1].getLastOutputKey();
        return cacheManager.get(key);
    }

    public Object getUpdateOutput() {
        String key = updateFlows[detectFlows.length - 1].getLastOutputKey();
        return cacheManager.get(key);
    }

    /**
     * Runs the violation repair execution.
     */
    public CleanExecutor detect() {
        // TODO: run multiple rules in parallel in process / thread.
        for (int i = 0; i < detectFlows.length; i ++) {
            detect(i);
        }
        return this;
    }

    /**
     * Runs the violation repair execution.
     */
    public CleanExecutor detect(int ruleIndex) {
        Preconditions.checkArgument(ruleIndex >= 0 && ruleIndex < cleanPlan.getRules().size());
        // TODO: run multiple rules in parallel in process / thread.
        Stopwatch stopwatch = new Stopwatch().start();
        Flow flow = detectFlows[ruleIndex];
        flow.start();
        Tracer.addStatEntry(
            Tracer.StatType.DetectTime,
            Long.toString(stopwatch.elapsed(TimeUnit.MILLISECONDS))
        );
        stopwatch.stop();
        return this;
    }

    /**
     * Runs the violation detection execution.
     */
    public CleanExecutor repair(int ruleIndex) {
        Preconditions.checkArgument(ruleIndex >= 0 && ruleIndex < cleanPlan.getRules().size());
        long elapseTime;
        // TODO: run multiple rules in parallel in process / thread.
        Stopwatch stopwatch = new Stopwatch().start();
        repairFlows[ruleIndex].start();
        elapseTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        Tracer.addStatEntry(
            Tracer.StatType.RepairTime,
            Long.toString(elapseTime)
        );

        updateFlows[ruleIndex].start();
        elapseTime = stopwatch.elapsed(TimeUnit.MILLISECONDS) - elapseTime;
        Tracer.addStatEntry(
            Tracer.StatType.EQTime,
            Long.toString(elapseTime)
        );
        stopwatch.stop();
        return this;
    }

    /**
     * Runs the violation detection execution.
     */
    public CleanExecutor repair() {
        // TODO: run multiple rules in parallel in process / thread.
        for (int i = 0; i < repairFlows.length; i ++) {
            repair(i);
        }

        return this;
    }

    /**
     * Run both the detection and repair.
     * @param ruleIndex rule index.
     * @return itself.
     */
    public CleanExecutor run(int ruleIndex) {
        detect(ruleIndex);
        repair(ruleIndex);
        return this;
    }

    /**
     * Run both the detection and repair.
     * @return itself.
     */
    public CleanExecutor run() {
        for (int i = 0; i < repairFlows.length; i ++) {
            detect(i);
            repair(i);
        }
        return this;
    }
    //</editor-fold>

    /**
     * Assemble the workflow.
     */
    private void assembleFlow() {
        List<Rule> rules = cleanPlan.getRules();
        int nRule = rules.size();
        detectFlows = new Flow[nRule];
        repairFlows = new Flow[nRule];
        updateFlows = new Flow[nRule];

        try {
            for (int i = 0; i < nRule; i ++)  {
                // assemble the detection flow.
                detectFlows[i] = new Flow();
                Rule rule = rules.get(i);
                String inputKey = cacheManager.put(rule, Integer.MAX_VALUE);
                detectFlows[i]
                    .setInputKey(inputKey)
                    .addNode(new SourceDeserializer(cleanPlan), "deserializer")
                    .addNode(new QueryEngine(rule), "query");
                if (rule.supportTwoInputs()) {
                    // the case where the rule is working on multiple tables (2).
                    detectFlows[i].addNode(new ViolationDetector<TuplePair>(rule), "detector");
                } else {
                    detectFlows[i].addNode(
                        new ViolationDetector<TupleCollection>(rule),
                        "detector"
                    );
                }
                detectFlows[i].addNode(new ViolationExport(cleanPlan), "export");

                // assemble the repair flow
                repairFlows[i] = new Flow();
                repairFlows[i]
                    .setInputKey(inputKey)
                    .addNode(new ViolationDeserializer(), "violation_deserializer")
                    .addNode(new ViolationRepair(rule), "violation_repair")
                    .addNode(new FixExport(cleanPlan), "fix export");

                // assemble the updator flow
                updateFlows[i] = new Flow();
                updateFlows[i]
                    .setInputKey(inputKey)
                    .addNode(new FixDeserializer(), "fix deserializer")
                    .addNode(new FixDecisionMaker(), "eq")
                    .addNode(new Updater(cleanPlan), "updater");
            }
        } catch (Exception ex) {
            tracer.err("Exception happens during assembling the pipeline " + ex.getMessage());
            if (Tracer.isInfoOn()) {
                ex.printStackTrace();
            }
        }
    }
}
