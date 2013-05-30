/*
 * Copyright (C) Qatar Computing Research Institute, 2013.
 * All rights reserved.
 */

package qa.qcri.nadeef.core.pipeline;

import com.google.common.base.Preconditions;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.tools.Tracer;

/**
 * CleanPlan execution logic. It assembles the right pipeline based on the clean plan and
 * start the execution.
 */
public class CleanExecutor {

    //<editor-fold desc="Private fields">
    private static Tracer tracer = Tracer.getTracer(CleanExecutor.class);
    private CleanPlan cleanPlan;
    private NodeCacheManager cacheManager;
    private Flow queryFlow;
    private Flow detectFlow;
    private Flow repairFlow;
    private Flow updateFlow;
    //</editor-fold>

    //<editor-fold desc="Constructor / Deconstructor">

    /**
     * Constructor.
     */
    public CleanExecutor(CleanPlan cleanPlan) {
        initialize(cleanPlan);
    }

    public void initialize(CleanPlan cleanPlan) {
        this.cleanPlan = Preconditions.checkNotNull(cleanPlan);
        this.cacheManager = NodeCacheManager.getInstance();
        assembleFlow();
    }

    @Override
    public void finalize() {
        if (queryFlow != null && queryFlow.isRunning()) {
            queryFlow.forceStop();
        }

        if (detectFlow != null && detectFlow.isRunning()) {
            detectFlow.forceStop();
        }

        if (repairFlow != null && repairFlow.isRunning()) {
            repairFlow.forceStop();
        }

        if (updateFlow != null && updateFlow.isRunning()) {
            updateFlow.forceStop();
        }

        queryFlow = null;
        detectFlow = null;
        repairFlow = null;
        updateFlow = null;
    }
    //</editor-fold>

    //<editor-fold desc="Public methods">
    public Object getDetectOutput() {
        String key = detectFlow.getCurrentOutputKey();
        return cacheManager.get(key);
    }

    public Object getRepairOutput() {
        String key = repairFlow.getCurrentOutputKey();
        return cacheManager.get(key);
    }

    public Object getUpdateOutput() {
        String key = updateFlow.getCurrentOutputKey();
        return cacheManager.get(key);
    }

    /**
     * Runs the violation detection. This is a non-blocking operation.
     */
    public CleanExecutor detect() {
        queryFlow.reset();
        detectFlow.reset();

        queryFlow.start();
        detectFlow.start();

        queryFlow.waitUntilFinish();
        detectFlow.waitUntilFinish();

        Tracer.putStatEntry(
            Tracer.StatType.DetectTime,
            queryFlow.getElapsedTime() + detectFlow.getElapsedTime()
        );

        return this;
    }

    public double getDetectPercentage() {
        return queryFlow.getPercentage() * 0.5 + detectFlow.getPercentage() * 0.5;
    }

    public double getRepairPercentage() {
        return repairFlow.getPercentage();
    }

    public double getRunPercentage() {
        return getDetectPercentage() * 0.5 + getRepairPercentage() * 0.5;
    }

    public CleanPlan getCleanPlan() {
        return cleanPlan;
    }

    /**
     * Runs the violation detection execution.
     */
    public CleanExecutor repair() {
        repairFlow.reset();
        updateFlow.reset();

        repairFlow.start();
        repairFlow.waitUntilFinish();

        Tracer.putStatEntry(Tracer.StatType.RepairTime, repairFlow.getElapsedTime());

        updateFlow.start();
        updateFlow.waitUntilFinish();

        Tracer.putStatEntry(Tracer.StatType.EQTime, updateFlow.getElapsedTime());
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
            detect();
            repair();

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
     * Assemble the workflow on demand.
     */
    @SuppressWarnings("unchecked")
    private void assembleFlow() {
        Rule rule = cleanPlan.getRule();

        try {
            String inputKey = cacheManager.put(rule, Integer.MAX_VALUE);
            // assemble the query flow.
            queryFlow = new Flow("query");
            queryFlow.setInputKey(inputKey).addNode(new SourceDeserializer(cleanPlan));

            if (rule.supportOneInput()) {
                queryFlow
                    .addNode(new ScopeOperator<Tuple>(rule))
                    .addNode(new Iterator<Tuple>(rule), 6);
            } else if (rule.supportTwoInputs()) {
                // the case where the rule is working on multiple tables (2).
                queryFlow
                    .addNode(new ScopeOperator<TuplePair>(rule))
                    .addNode(new Iterator<TuplePair>(rule), 6);
            } else {
                queryFlow
                    .addNode(new ScopeOperator<Tuple>(rule))
                    .addNode(new Iterator<TupleCollection>(rule), 6);
            }

            // assemble the detect flow
            detectFlow = new Flow("detect");
            detectFlow.setInputKey(inputKey);
            if (rule.supportTwoInputs()) {
                detectFlow.addNode(new ViolationDetector<TuplePair>(rule), 6);
            } else {
                detectFlow.addNode(new ViolationDetector<TupleCollection>(rule), 6);
            }
            detectFlow.addNode(new ViolationExport(cleanPlan));

            // assemble the repair flow
            repairFlow = new Flow("repair");
            repairFlow.setInputKey(inputKey)
                .addNode(new ViolationDeserializer())
                .addNode(new ViolationRepair(rule), 6)
                .addNode(new FixExport(cleanPlan));

            // assemble the updater flow
            updateFlow = new Flow("update");
            updateFlow.setInputKey(inputKey)
                .addNode(new FixDeserializer())
                .addNode(new FixDecisionMaker(), 6)
                .addNode(new Updater());
        } catch (Exception ex) {
            tracer.err("Exception happens during assembling the pipeline ", ex);
            if (Tracer.isInfoOn()) {
                ex.printStackTrace();
            }
        }
    }
    //</editor-fold>
}
