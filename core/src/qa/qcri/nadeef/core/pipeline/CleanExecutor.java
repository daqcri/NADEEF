/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.operator.*;
import qa.qcri.nadeef.tools.Tracer;

import java.util.Collection;
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
    private List<Flow> detectFlows;
    private List<Flow> repairFlows;
    private List<Flow> updateFlows;

    //<editor-fold desc="Constructor">
    /**
     * Constructor.
     * @param cleanPlan clean plan.
     */
    public CleanExecutor(CleanPlan cleanPlan) {
        this.cleanPlan = Preconditions.checkNotNull(cleanPlan);
        this.cacheManager = NodeCacheManager.getInstance();
        detectFlows = Lists.newArrayList();
        repairFlows = Lists.newArrayList();
        updateFlows = Lists.newArrayList();
    }
    //</editor-fold>

    //<editor-fold desc="Public methods">
    public Object getDetectOutput() {
        String key = detectFlows.get(detectFlows.size() - 1).getLastOutputKey();
        return cacheManager.get(key);
    }

    public Object getRepairOutput() {
        String key = repairFlows.get(repairFlows.size() - 1).getLastOutputKey();
        return cacheManager.get(key);
    }

    public Object getUpdateOutput() {
        String key = updateFlows.get(updateFlows.size() - 1).getLastOutputKey();
        return cacheManager.get(key);
    }

    /**
     * Runs the violation repair execution.
     */
    public CleanExecutor detect() {
        // TODO: run multiple rules in parallel in process / thread.
        int nRule = getRuleSize();
        for (int i = 0; i < nRule; i ++) {
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
        getDetectFlow(ruleIndex).start();
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
        getRepairFlow(ruleIndex).start();
        elapseTime = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        Tracer.addStatEntry(
            Tracer.StatType.RepairTime,
            Long.toString(elapseTime)
        );

        getUpdateFlow(ruleIndex).start();
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
        int size = getRuleSize();
        for (int i = 0; i < size; i ++) {
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
        int changedCells = 0;
        int count = 0;
        do {
            tracer.verbose("Running iteration " + count + 1);
            int size = getRuleSize();
            for (int i = 0; i < size; i ++) {
                detect(i);
                repair(i);
            }

            changedCells = ((Integer)getUpdateOutput()).intValue();
            count ++;
            if (count == NadeefConfiguration.getMaxIterationNumber()) {
                break;
            }
        } while (changedCells != 0);
        return this;
    }
    //</editor-fold>

    //<editor-fold desc="Private members">
    /**
     * Gets the number of rules in the executor.
     * @return number of rules.
     */
    private int getRuleSize() {
        return cleanPlan.getRules().size();
    }

    /**
     * Gets the DetectFlow.
     */
    private synchronized Flow getDetectFlow(int index) {
        Preconditions.checkArgument(index < getRuleSize());
        if (detectFlows.size() <= index) {
            assembleFlow();
        }
        return detectFlows.get(index);
    }

    /**
     * Gets the DetectFlow.
     */
    private synchronized Flow getRepairFlow(int index) {
        Preconditions.checkArgument(index < getRuleSize());
        if (repairFlows.size() <= index) {
            assembleFlow();
        }
        return repairFlows.get(index);
    }

    /**
     * Gets the DetectFlow.
     */
    private synchronized Flow getUpdateFlow(int index) {
        Preconditions.checkArgument(index < getRuleSize());
        if (updateFlows.size() <= index) {
            assembleFlow();
        }
        return updateFlows.get(index);
    }

    /**
     * Assemble the workflow on demand.
     */
    private void assembleFlow() {
        List<Rule> rules = cleanPlan.getRules();
        int nRule = rules.size();

        try {
            Flow flow = null;
            for (int i = 0; i < nRule; i ++)  {
                Rule rule = rules.get(i);
                String inputKey = cacheManager.put(rule, Integer.MAX_VALUE);
                // assemble the detection flow.
                if (detectFlows.size() <= i) {
                    flow = new Flow();
                    flow.setInputKey(inputKey)
                        .addNode(new SourceDeserializer(cleanPlan), "deserializer");
                    if (rule.supportTwoInputs()) {
                        // the case where the rule is working on multiple tables (2).
                        flow.addNode(
                            new QueryEngine<TuplePair, Collection<TuplePair>>(rule), "query"
                        ).addNode(new ViolationDetector<TuplePair>(rule), "detector");
                    } else {
                        flow.addNode(
                            new QueryEngine<Tuple, Collection<TupleCollection>>(rule), "query"
                        ).addNode(new ViolationDetector<TupleCollection>(rule), "detector");
                    }
                    flow.addNode(new ViolationExport(cleanPlan), "export");
                    detectFlows.add(flow);
                }

                if (repairFlows.size() <= i) {
                    // assemble the repair flow
                    flow = new Flow();
                    flow.setInputKey(inputKey)
                        .addNode(new ViolationDeserializer(), "violation_deserializer")
                        .addNode(new ViolationRepair(rule), "violation_repair")
                        .addNode(new FixExport(cleanPlan), "fix export");
                    repairFlows.add(flow);
                }

                if (updateFlows.size() <= i) {
                    // assemble the updater flow
                    flow = new Flow();
                    flow.setInputKey(inputKey)
                        .addNode(new FixDeserializer(), "fix deserializer")
                        .addNode(new FixDecisionMaker(), "eq")
                        .addNode(new Updater(cleanPlan), "updater");
                    updateFlows.add(flow);
                }
            }
        } catch (Exception ex) {
            tracer.err("Exception happens during assembling the pipeline " + ex.getMessage());
            if (Tracer.isInfoOn()) {
                ex.printStackTrace();
            }
        }
    }
    //</editor-fold>
}
