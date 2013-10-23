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
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.tools.DBConfig;
import qa.qcri.nadeef.tools.Tracer;

/**
 * Updater flow executor.
 */
public class UpdateExecutor {
    private Flow updateFlow;
    private NodeCacheManager cacheManager;
    private Tracer tracer;

    public UpdateExecutor(DBConfig dbConfig) {
        cacheManager = NodeCacheManager.getInstance();
        tracer = Tracer.getTracer(UpdateExecutor.class);
        assembleFlow(dbConfig);
    }

    public void shutdown() {
        if (updateFlow != null && updateFlow.isRunning()) {
            updateFlow.forceStop();
        }
    }

    @Override
    public void finalize() throws Throwable{
        shutdown();
        super.finalize();
    }

    /**
     * Gets the updated cell count.
     * @return updated cell count.
     */
    public int getUpdateCellCount() {
        String key = updateFlow.getCurrentOutputKey();
        Object output = cacheManager.get(key);
        return (Integer) output;
    }

    public void run() {
        updateFlow.reset();
        updateFlow.start();
        updateFlow.waitUntilFinish();

        Tracer.putStatsEntry(Tracer.StatType.EQTime, updateFlow.getElapsedTime());
    }

    @SuppressWarnings("unchecked")
    private void assembleFlow(DBConfig dbConfig) {
        try {
            // assemble the updater flow
            updateFlow = new Flow("update");
            Optional<Class> eqClass = NadeefConfiguration.getDecisionMakerClass();
            // check whether user provides a customized DecisionMaker class, if so, replace it
            // with default EQ class.
            FixDecisionMaker fixDecisionMaker = null;
            if (eqClass.isPresent()) {
                Class customizedClass = eqClass.get();
                if (!FixDecisionMaker.class.isAssignableFrom(customizedClass)) {
                    throw
                        new IllegalArgumentException(
                            "FixDecisionMaker class is not a class inherit from FixDecisionMaker"
                        );
                }

                fixDecisionMaker =
                    (FixDecisionMaker)customizedClass.getConstructor().newInstance();
            } else {
                fixDecisionMaker = new EquivalentClass();
            }

            updateFlow.setInputKey(cacheManager.getDummyKey())
                .addNode(new FixImport(NadeefConfiguration.getDbConfig()))
                .addNode(fixDecisionMaker, 6)
                .addNode(new Updater(dbConfig, NadeefConfiguration.getDbConfig()));
        } catch (Exception ex) {
            tracer.err("Exception happens during assembling the update flow.", ex);
        }
    }
}
