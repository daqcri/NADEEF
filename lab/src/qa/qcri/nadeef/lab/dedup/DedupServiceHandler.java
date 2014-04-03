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

package qa.qcri.nadeef.lab.dedup;

import com.google.common.collect.Lists;
import org.apache.thrift.TException;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;
import qa.qcri.nadeef.core.pipeline.UpdateExecutor;
import qa.qcri.nadeef.tools.Tracer;

import java.io.FileReader;
import java.util.HashSet;
import java.util.List;

public class DedupServiceHandler {
    private Tracer tracer = Tracer.getTracer(DedupServiceHandler.class);

    public void cureMissingValue(List<Integer> newItems) throws TException {
        tracer.info("-- Start Data cure --");
        CleanExecutor executor = null;
        int count;
        List<Integer> result = Lists.newArrayList();
        try {
            CleanPlan cleanPlan =
                CleanPlan.createCleanPlanFromJSON(
                    new FileReader("lab/src/qa/qcri/nadeef/lab/dedup/CureMissing.json"),
                    NadeefConfiguration.getDbConfig()
                ).get(0);

            List<String> tableNames = cleanPlan.getRule().getTableNames();
            String tableName = tableNames.get(0);

            HashSet<Integer> set = new HashSet<>();
            for (int tid : newItems) {
                set.add(tid);
            }

            executor = new CleanExecutor(cleanPlan);
            if (!set.isEmpty()) {
                executor.incrementalAppend(tableName, set);
            }

            executor.detect();
            count = executor.getDetectViolation().size();
            tracer.info("Found " + count + " tuples with missing value.");
            executor.repair();
            UpdateExecutor updateExecutor = new UpdateExecutor(cleanPlan);
            updateExecutor.run();
            count = updateExecutor.getUpdateCellCount();
            tracer.info("Cured " + count + " items.");
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }
    }

    public List<Integer> incrementalDedup(List<Integer> newItems)
        throws TException {
        CleanExecutor executor = null;
        List<Integer> result = Lists.newArrayList();
        int count;
        try {
            CleanPlan cleanPlan =
                CleanPlan.createCleanPlanFromJSON(
                    new FileReader("lab/src/qa/qcri/nadeef/lab/dedup/DedupPlan.json"),
                    NadeefConfiguration.getDbConfig()
                ).get(0);

            List<String> tableNames = cleanPlan.getRule().getTableNames();
            String tableName = tableNames.get(0);
            executor = new CleanExecutor(cleanPlan);

            HashSet<Integer> set = new HashSet<>();
            for (int tid : newItems) {
                set.add(tid);
            }

            if (!set.isEmpty()) {
                executor.incrementalAppend(tableName, set);
            }

            executor.detect();
            count = executor.getDetectViolation().size();
            tracer.info("Found " + count + " tuples with duplications.");
            executor.repair();

            UpdateExecutor updateExecutor = new UpdateExecutor(cleanPlan);
            updateExecutor.run();
            tracer.info("Deduped " + count + " items.");
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            if (executor != null) {
                executor.shutdown();
            }
        }

        return result;
    }
}
