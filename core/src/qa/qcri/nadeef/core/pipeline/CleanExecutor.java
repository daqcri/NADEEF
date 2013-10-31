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

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import qa.qcri.nadeef.core.datamodel.*;
import qa.qcri.nadeef.core.util.sql.DBConnectionPool;
import qa.qcri.nadeef.core.util.sql.DBInstaller;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;

import java.util.List;

/**
 * CleanPlan execution logic. It assembles the right pipeline based on the clean plan and
 * drives the cleaning execution.
 */
public class CleanExecutor {

    //<editor-fold desc="Private fields">
    private static Tracer tracer = Tracer.getTracer(CleanExecutor.class);
    private CleanPlan cleanPlan;
    private NodeCacheManager cacheManager;
    private Flow queryFlow;
    private Flow detectFlow;
    private Flow repairFlow;
    private DBConnectionPool connectionPool;
    private CleanExecutorContext context;
    //</editor-fold>

    //<editor-fold desc="Constructor / Deconstructor">

    /**
     * Constructor. Use default NADEEF default config as DB config.
     * @param cleanPlan input {@link CleanPlan}.
     */
    public CleanExecutor(CleanPlan cleanPlan) throws Exception {
        this(cleanPlan, NadeefConfiguration.getDbConfig());
    }

    /**
     * Constructor.
     * @param cleanPlan input {@link CleanPlan}.
     * @param dbConfig meta data dbconfig.
     */
    public CleanExecutor(CleanPlan cleanPlan, DBConfig dbConfig) throws Exception {
        this.cleanPlan = Preconditions.checkNotNull(cleanPlan);
        this.cacheManager = NodeCacheManager.getInstance();
        this.connectionPool =
            DBConnectionPool.createDBConnectionPool(
                cleanPlan.getSourceDBConfig(),
                dbConfig
            );
        DBInstaller.install(dbConfig);
        assembleFlow();
    }

    /**
     * Returns <code>True</code> when the clean executor is running.
     * @return <code>True</code> when the clean executor is running.
     */
    public synchronized boolean isRunning() {
        return detectFlow.isRunning() ||
               queryFlow.isRunning() ||
               repairFlow.isRunning();
    }

    /**
     * Shutdown the CleanExecutor.
     */
    public void shutdown() {
        if (queryFlow != null) {
            if (queryFlow.isRunning()) {
                queryFlow.forceStop();
            }
            cacheManager.remove(queryFlow.getInputKey());
        }

        queryFlow = null;

        if (detectFlow != null) {
            if (detectFlow.isRunning()) {
                detectFlow.forceStop();
            }
            cacheManager.remove(detectFlow.getInputKey());
        }
        detectFlow = null;

        if (repairFlow != null) {
            if (repairFlow.isRunning()) {
                repairFlow.forceStop();
            }
            cacheManager.remove(repairFlow.getInputKey());
        }

        if (connectionPool != null) {
            connectionPool.shutdown();
        }
        connectionPool = null;
    }

    /**
     * CleanExecutor finalizer.
     */
    @Override
    public void finalize() throws Throwable{
        shutdown();
        super.finalize();
    }
    //</editor-fold>

    //<editor-fold desc="Public methods">

    /**
     * Gets the connection pool.
     * @return connection pool.
     */
    public DBConnectionPool getConnectionPool() {
        return connectionPool;
    }

    /**
     * Gets the output from Detect.
     * @return output object from Detect.
     */
    public Object getDetectOutput() {
        String key = detectFlow.getCurrentOutputKey();
        return cacheManager.get(key);
    }

    /**
     * Gets the output from Repair.
     * @return output object from repair.
     */
    public Object getRepairOutput() {
        String key = repairFlow.getCurrentOutputKey();
        return cacheManager.get(key);
    }

    /**
     * Gets the current progress percentage of Detect.
     * @return current progress percentage of Detect.
     */
    public double getDetectProgress() {
        return queryFlow.getProgress() * 0.5 + detectFlow.getProgress() * 0.5;
    }

    /**
     * Gets the detail progress information of Detection.
     * @return the detail progress information of Detection.
     */
    public List<ProgressReport> getDetailDetectProgress() {
        List<ProgressReport> queryProgress = queryFlow.getDetailProgress();
        List<ProgressReport> detectProgress = detectFlow.getDetailProgress();
        queryProgress.addAll(detectProgress);
        return queryProgress;
    }

    /**
     * Gets the current percentage of Repair.
     * @return current percentage of Repair.
     */
    public double getRepairProgress() {
        return repairFlow.getProgress();
    }

    /**
     * Gets the detail progress information of Detection.
     * @return the detail progress information of Detection.
     */
    public List<ProgressReport> getDetailRepairProgress() {
        return repairFlow.getDetailProgress();
    }

    /**
     * Runs the violation detection.
     */
    public CleanExecutor detect() {
        queryFlow.reset();
        detectFlow.reset();

        queryFlow.start();
        detectFlow.start();

        queryFlow.waitUntilFinish();
        detectFlow.waitUntilFinish();

        Tracer.appendMetric(
            Tracer.Metric.DetectPipelineTime,
            queryFlow.getElapsedTime() + detectFlow.getElapsedTime()
        );

        System.gc();
        return this;
    }



    /**
     * Gets the CleanPlan.
     * @return the CleanPlan.
     */
    public CleanPlan getCleanPlan() {
        return cleanPlan;
    }

    /**
     * Runs the violation repair.
     */
    public CleanExecutor repair() {
        repairFlow.reset();

        repairFlow.start();
        repairFlow.waitUntilFinish();

        Tracer.appendMetric(Tracer.Metric.RepairTime, repairFlow.getElapsedTime());

        System.gc();
        return this;
    }

    /**
     * Runs both the detection and repair.
     */
    public synchronized CleanExecutor run() {
        detect();
        repair();
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
            String inputKey = cacheManager.put(Optional.absent());
            // assemble the query flow.
            queryFlow = new Flow("query");
            queryFlow
                .setInputKey(inputKey)
                .addNode(new SourceImport());

            if (rule.supportOneTuple()) {
                queryFlow
                    .addNode(new ScopeOperator<Tuple>())
                    .addNode(new Iterator<Tuple>(), 6);
            } else if (rule.supportTwoTuples()) {
                // the case where the rule is working on multiple tables (2).
                queryFlow
                    .addNode(new ScopeOperator<TuplePair>())
                    .addNode(new Iterator<TuplePair>(), 6);
            } else {
                queryFlow
                    .addNode(new ScopeOperator<Tuple>())
                    .addNode(new Iterator<Table>(), 6);
            }

            // assemble the detect flow
            detectFlow = new Flow("detect");
            detectFlow.setInputKey(inputKey);
            if (rule.supportTwoTuples()) {
                detectFlow.addNode(new ViolationDetector<TuplePair>(), 6);
            } else {
                detectFlow.addNode(new ViolationDetector<Table>(), 6);
            }
            detectFlow.addNode(new ViolationExport());

            // assemble the repair flow
            repairFlow = new Flow("repair");
            repairFlow.setInputKey(inputKey)
                .addNode(new ViolationImport())
                .addNode(new ViolationRepair(rule), 6)
                .addNode(new FixExport());

        } catch (Exception ex) {
            tracer.err("Exception happens during assembling the pipeline ", ex);
        }
    }
    //</editor-fold>
}
