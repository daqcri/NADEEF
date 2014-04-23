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
import qa.qcri.nadeef.core.datamodel.Cell;
import qa.qcri.nadeef.core.datamodel.CleanPlan;
import qa.qcri.nadeef.core.datamodel.NadeefConfiguration;
import qa.qcri.nadeef.core.datamodel.Violation;
import qa.qcri.nadeef.core.pipeline.CleanExecutor;

import java.io.FileReader;
import java.util.HashSet;
import java.util.List;

public class DedupServiceHandler implements TDedupService.Iface {
    @Override
    public List<List<Integer>> incrementalDedup(List<Integer> newItems)
        throws TException {
        CleanExecutor executor = null;
        List<List<Integer>> result = Lists.newArrayList();
        try {
            CleanPlan cleanPlan =
                CleanPlan.create(
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
            executor.incrementalAppend(tableName, set);
            executor.detect();
            List<Violation> violations = executor.getDetectViolation();
            for (Violation v : violations) {
                List<Cell> cells = Lists.newArrayList(v.getCells());
                List<Integer> tmp = Lists.newArrayList();
                for (Cell cell : cells) {
                    tmp.add(cell.<Integer>getValue());
                }
                if (tmp.size() != 0) {
                    result.add(tmp);
                }
            }
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
